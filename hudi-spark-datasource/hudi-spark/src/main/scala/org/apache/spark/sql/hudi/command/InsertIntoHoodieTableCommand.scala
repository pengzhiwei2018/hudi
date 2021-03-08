/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.command

import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodieRecord}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.exception.HoodieDuplicateKeyException
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.keygen.{ComplexKeyGenerator, UuidKeyGenerator}
import org.apache.hudi.{HoodieSparkSqlWriter, HoodieWriterUtils}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, LogicalRelation}
import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructType}

/**
  * Command for insert into hoodie table.
  */
class InsertIntoHoodieTableCommand(
    logicalRelation: LogicalRelation,
    query: LogicalPlan,
    partition: Map[String, Option[String]],
    overwrite: Boolean)
  extends InsertIntoDataSourceCommand(logicalRelation, query, overwrite) {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(logicalRelation.catalogTable.isDefined, "Missing catalog table")

    val table = logicalRelation.catalogTable.get
    InsertIntoHoodieTableCommand.run(sparkSession, table, query, partition, overwrite)
    Seq.empty[Row]
  }
}

object InsertIntoHoodieTableCommand {
  /**
    * Run the insert query. We support both dynamic partition insert and static partition insert.
    * @param sparkSession The spark session.
    * @param table The insert table.
    * @param query The insert query.
    * @param insertPartitions The specified insert partition map.
    *                         e.g. "insert into h(dt = '2021') select id, name from src"
    *                         "dt" is the key in the map and "2021" is the partition value. If the
    *                         partition value has not specified(in the case of dynamic partition)
    *                         , it is None in the map.
    * @param overwrite Whether to overwrite the table.
    */
  def run(sparkSession: SparkSession, table: CatalogTable, query: LogicalPlan,
          insertPartitions: Map[String, Option[String]],
          overwrite: Boolean): Unit = {

    val config = if (table.schema.fields.nonEmpty) { // for insert into
      buildHoodieInsertConfig(table, sparkSession, insertPartitions)
    } else { // It is CTAS if the table schema is empty, we use the schema from the query.
      buildHoodieInsertConfig(table, sparkSession, insertPartitions, Some(query.schema))
    }

    val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
    val parameters = HoodieWriterUtils.parametersWithWriteDefaults(config)
    val queryData = Dataset.ofRows(sparkSession, query)
    val conf = sparkSession.sessionState.conf
    val alignedQuery = alignOutputFields(queryData, table, insertPartitions, conf)
    HoodieSparkSqlWriter.write(sparkSession.sqlContext, mode, parameters, alignedQuery)
    sparkSession.catalog.refreshTable(table.identifier.unquotedString)
  }

  /**
    * Aligned the type and name of query's output fields with the result table's fields.
    * @param query The insert query which to aligned.
    * @param table The result table.
    * @param insertPartitions The insert partition map.
    * @param conf The SQLConf.
    * @return
    */
  private def alignOutputFields(
    query: DataFrame,
    table: CatalogTable,
    insertPartitions: Map[String, Option[String]],
    conf: SQLConf): DataFrame = {

    val targetPartitionSchema = table.partitionSchema

    val staticPartitionValues = insertPartitions.filter(p => p._2.isDefined).mapValues(_.get)
    assert(staticPartitionValues.isEmpty ||
      staticPartitionValues.size == targetPartitionSchema.size,
      s"Required partition columns is: ${targetPartitionSchema.json}, Current static partitions " +
        s"is: ${staticPartitionValues.mkString("," + "")}")

    val queryDataFields = if (staticPartitionValues.isEmpty) { // insert dynamic partition
      query.logicalPlan.output.dropRight(targetPartitionSchema.fields.length)
    } else { // insert static partition
      query.logicalPlan.output
    }
    val targetDataSchema = if (table.schema.fields.nonEmpty) {
      table.dataSchema
    } else { // for CTAS
      query.schema
    }
    // Align for the data fields of the query
    val dataProjects = queryDataFields.zip(targetDataSchema.fields).map {
      case (dataAttr, targetField) =>
        val castAttr = castIfNeeded(dataAttr,
          targetField.dataType, conf)
        new Column(Alias(castAttr, targetField.name)())
    }

    val partitionProjects = if (staticPartitionValues.isEmpty) { // insert dynamic partitions
      // The partition attributes is followed the data attributes in the query
      // So we init the partitionAttrPosition with the data schema size.
      var partitionAttrPosition = targetDataSchema.size
      targetPartitionSchema.fields.map(f => {
        val partitionAttr = query.logicalPlan.output(partitionAttrPosition)
        partitionAttrPosition = partitionAttrPosition + 1
        // Cast the partition attribute to the target table field's type.
        val castAttr = castIfNeeded(partitionAttr, f.dataType, conf)
        new Column(Alias(castAttr, f.name)())
      })
    } else { // insert static partitions
      targetPartitionSchema.fields.map(f => {
        val staticPartitionValue = staticPartitionValues.getOrElse(f.name,
        s"Missing static partition value for: ${f.name}")
        // Cast the static partition value to the target table field's type.
        val castAttr = castIfNeeded(
          Literal.create(staticPartitionValue, StringType), f.dataType, conf)
        new Column(Alias(castAttr, f.name)())
      })
    }
    val alignedProjects = dataProjects ++ partitionProjects
    query.select(alignedProjects: _*)
  }

  /**
    * Build the default config for insert.
    * @return
    */
  private def buildHoodieInsertConfig(table: CatalogTable,
                              sparkSession: SparkSession,
                              insertPartitions: Map[String, Option[String]] = Map.empty,
                              schema: Option[StructType] = None): Map[String, String] = {

    if (insertPartitions.nonEmpty &&
      (insertPartitions.keys.toSet != table.partitionColumnNames.toSet)) {
      throw new IllegalArgumentException(s"Insert partition fields" +
        s"[${insertPartitions.keys.mkString(" " )}]" +
        s" not equal to the defined partition in table[${table.partitionColumnNames.mkString(",")}]")
    }
    val parameters = HoodieOptionConfig.mappingSqlOptionToHoodieParam(table.storage.properties)

    val tableType = parameters.getOrElse(TABLE_TYPE_OPT_KEY, DEFAULT_TABLE_TYPE_OPT_VAL)

    val partitionFields = table.partitionColumnNames.mkString(",")
    val path = getTableLocation(table, sparkSession)
      .getOrElse(s"Missing location for table ${table.identifier}")

    val tableSchema = schema.getOrElse(table.schema)
    val options = table.storage.properties
    val primaryColumns = HoodieOptionConfig.getPrimaryColumns(options)

    val keyGenClass = if (primaryColumns.nonEmpty) {
      classOf[ComplexKeyGenerator].getCanonicalName
    } else {
      classOf[UuidKeyGenerator].getName
    }

    val dropDuplicate = sparkSession.conf
      .getOption(HoodieOptionConfig.withPrefix(INSERT_DROP_DUPS_OPT_KEY))
      .getOrElse(DEFAULT_INSERT_DROP_DUPS_OPT_VAL)
      .toBoolean

    val operation = if (primaryColumns.nonEmpty && !dropDuplicate) {
      UPSERT_OPERATION_OPT_VAL
    } else {
      INSERT_OPERATION_OPT_VAL
    }

    val payloadClassName = if (primaryColumns.nonEmpty && !dropDuplicate) {
      classOf[ValidateDuplicateKeyPayload].getCanonicalName
    } else {
      classOf[DefaultHoodieRecordPayload].getCanonicalName
    }

    withSparkConf(sparkSession, options) {
      Map(
        "path" -> removeStarFromPath(path),
        TABLE_TYPE_OPT_KEY -> tableType,
        TABLE_NAME -> table.identifier.table,
        PRECOMBINE_FIELD_OPT_KEY -> tableSchema.fields.last.name,
        OPERATION_OPT_KEY -> operation,
        KEYGENERATOR_CLASS_OPT_KEY -> keyGenClass,
        RECORDKEY_FIELD_OPT_KEY -> primaryColumns.mkString(","),
        PARTITIONPATH_FIELD_OPT_KEY -> partitionFields,
        PAYLOAD_CLASS_OPT_KEY -> payloadClassName,
        META_SYNC_ENABLED_OPT_KEY -> "true",
        HIVE_USE_JDBC_OPT_KEY -> "false",
        HIVE_DATABASE_OPT_KEY -> table.identifier.database.getOrElse("default"),
        HIVE_TABLE_OPT_KEY -> table.identifier.table,
        HIVE_PARTITION_FIELDS_OPT_KEY -> partitionFields,
        HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> classOf[MultiPartKeysValueExtractor].getCanonicalName,
        URL_ENCODE_PARTITIONING_OPT_KEY -> "true"
      )
    }
  }
}

/**
  * Validate the duplicate key for insert statement without enable the INSERT_DROP_DUPS_OPT_KEY
  * config.
  */
class ValidateDuplicateKeyPayload(record: GenericRecord, orderingVal: Comparable[_])
  extends DefaultHoodieRecordPayload(record, orderingVal) {

  def this(record: HOption[GenericRecord]) {
    this(if (record.isPresent) record.get else null, 0)
  }

  override def combineAndGetUpdateValue(currentValue: IndexedRecord,
                               schema: Schema, properties: Properties): HOption[IndexedRecord] = {
    val key = currentValue.asInstanceOf[GenericRecord].get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString
    throw new HoodieDuplicateKeyException(key)
  }
}
