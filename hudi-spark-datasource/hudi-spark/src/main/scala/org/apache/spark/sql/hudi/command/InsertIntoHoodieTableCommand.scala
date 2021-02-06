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

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieStorageConfig.PARQUET_COMPRESSION_CODEC
import org.apache.hudi.config.HoodieWriteConfig.{INSERT_PARALLELISM, TABLE_NAME, UPSERT_PARALLELISM}
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.keygen.{ComplexKeyGenerator, UuidKeyGenerator}
import org.apache.hudi.{HoodieSparkSqlWriter, HoodieWriterUtils}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, LogicalRelation}
import org.apache.spark.sql.hudi.SQLConfig
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

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
    *
    * @param sparkSession
    * @param table
    * @param query
    * @param insertPartitions
    * @param overwrite
    */
  def run(sparkSession: SparkSession, table: CatalogTable, query: LogicalPlan,
          insertPartitions: Map[String, Option[String]],
          overwrite: Boolean): Unit = {

    val config = if (table.schema.fields.nonEmpty) {
      buildHoodieInsertConfig(table, sparkSession, insertPartitions)
    } else { // for CTAS
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
    * @param queryData The insert query which to aligned.
    * @param table The result table.
    * @param insertPartitions The insert partition map.
    * @param conf The SQLConf.
    * @return
    */
  private def alignOutputFields(
    queryData: DataFrame,
    table: CatalogTable,
    insertPartitions: Map[String, Option[String]],
    conf: SQLConf): DataFrame = {
    val queryDataFields =
      queryData.logicalPlan.output.dropRight(insertPartitions.values.count(_.isEmpty))

    val resultDataSchema = if (table.schema.fields.nonEmpty) {
      table.dataSchema
    } else { // for CTAS
      queryData.schema
    }
    val partitionSchema = if (table.schema.fields.nonEmpty) {
      table.partitionSchema
    } else { // for CTAS
      new StructType()
    }
    // Align for the data fields of the query
    val dataProjects = queryDataFields.zip(resultDataSchema.fields).map {
      case (dataAttr, targetField) =>
        val castAttr = castIfNeeded(dataAttr,
          targetField.dataType, conf)
        new Column(Alias(castAttr, targetField.name)())
    }
    // Align for the partition fields of the query
    var pos = resultDataSchema.size
    val partitionProjects = partitionSchema.fields.map(f => {
      val partitionVal = insertPartitions.get(f.name)
      if (partitionVal.isEmpty || partitionVal.get.isEmpty) {
        if (pos >= queryData.schema.fields.length) {
          throw new RuntimeException("Missing partition value or select expression for partition" +
            s" column ${f.name}")
        }
        val partitionAttr = queryData.logicalPlan.output(pos)
        pos = pos + 1
        val castAttr = castIfNeeded(partitionAttr, f.dataType, conf)
        new Column(Alias(castAttr, f.name)())
      } else {
        new Column(
          Alias(Literal(partitionVal.get.get), f.name)()
        )
      }
    })
    if (pos != queryData.schema.fields.length) {
      throw new IllegalArgumentException(s"Number of the query project" +
        s"[${queryData.schema.fields.length}] is not equal to  the required size:" +
        s" ${resultDataSchema.fields.length + partitionSchema.fields.length -
          insertPartitions.values.count(_.isDefined)}")
    }
    val projects = dataProjects ++ partitionProjects
    queryData.select(projects: _*)
  }

  /**
    * Build the default config for insert.
    * @param table
    * @param sparkSession
    * @param insertPartitions
    * @param schema
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
    val parameters = SQLConfig.mappingSqlOptionToHoodieParam(table.storage.properties)

    val tableType = parameters.getOrElse(TABLE_TYPE_OPT_KEY, DEFAULT_TABLE_TYPE_OPT_VAL)

    val partitionFields = table.partitionColumnNames.mkString(",")
    val path = getTableLocation(table, sparkSession)
      .getOrElse(s"Missing location for table ${table.identifier}")

    val tableSchema = schema.getOrElse(table.schema)
    val options = table.storage.properties
    val primaryColumns = SQLConfig.getPrimaryColumns(options)
    //
    val keyGenClass = if (primaryColumns.nonEmpty) {
      classOf[ComplexKeyGenerator].getCanonicalName
    } else {
      classOf[UuidKeyGenerator].getName
    }

    val operation = if (primaryColumns.nonEmpty) {
      UPSERT_OPERATION_OPT_VAL
    } else {
      INSERT_OPERATION_OPT_VAL
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
        META_SYNC_ENABLED_OPT_KEY -> "false", // TODO enable meta sync by default
        HIVE_USE_JDBC_OPT_KEY -> "false",
        HIVE_DATABASE_OPT_KEY -> table.identifier.database.getOrElse("default"),
        HIVE_TABLE_OPT_KEY -> table.identifier.table,
        HIVE_PARTITION_FIELDS_OPT_KEY -> partitionFields,
        HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> classOf[MultiPartKeysValueExtractor].getCanonicalName,
        URL_ENCODE_PARTITIONING_OPT_KEY -> "true",
        PARQUET_COMPRESSION_CODEC -> "SNAPPY",
        INSERT_PARALLELISM -> "64",
        UPSERT_PARALLELISM -> "64"
      )
    }
  }
}
