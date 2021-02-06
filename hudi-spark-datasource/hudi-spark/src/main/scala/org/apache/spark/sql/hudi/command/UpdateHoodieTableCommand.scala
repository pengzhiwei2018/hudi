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

import scala.collection.JavaConverters._

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{HIVE_DATABASE_OPT_KEY, HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, HIVE_PARTITION_FIELDS_OPT_KEY, HIVE_TABLE_OPT_KEY, HIVE_USE_JDBC_OPT_KEY, KEYGENERATOR_CLASS_OPT_KEY, META_SYNC_ENABLED_OPT_KEY, OPERATION_OPT_KEY, PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY, URL_ENCODE_PARTITIONING_OPT_KEY}
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.config.HoodieWriteConfig.{INSERT_PARALLELISM, TABLE_NAME, UPSERT_PARALLELISM}
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.{Column, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.hudi.SQLConfig
import org.apache.spark.sql.hudi.logical.{Assignment, UpdateTable}
import org.apache.spark.sql.types.StructField

case class UpdateHoodieTableCommand(updateTable: UpdateTable) extends RunnableCommand {

  private val table = updateTable.table
  private val tableAlias = table match {
    case SubqueryAlias(name, _) => name
    case _ => throw new IllegalArgumentException(s"Illegal table: $table")
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logInfo(s"start execute update command for $tableAlias")
    def cast(exp:Expression, field: StructField): Expression = {
      castIfNeeded(exp, field.dataType, sparkSession.sqlContext.conf)
    }
    val name2UpdateValue = updateTable.assignments.map {
      case Assignment(attr: AttributeReference, value) =>
        attr.name -> value
    }.toMap

    val updateExpressions = table.output
      .map(attr => name2UpdateValue.getOrElse(attr.name, attr))
      .filter { // filter the meta columns
        case attr: AttributeReference =>
          !HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet.contains(attr.name)
        case _=> true
      }

    val projects = updateExpressions.zip(removeMetaFields(table.schema).fields).map {
      case (attr: AttributeReference, field) =>
        Column(cast(attr, field))
      case (exp, field) =>
        Column(Alias(cast(exp, field), field.name)())
    }

    var df = Dataset.ofRows(sparkSession, table)
    if (updateTable.condition.isDefined) {
      df = df.filter(Column(updateTable.condition.get))
    }
    df = df.select(projects: _*)
    val config = buildHoodieConfig(sparkSession)
    df.write
      .format("hudi")
      .mode(SaveMode.Append)
      .options(config)
      .save()
    table.refresh()
    logInfo(s"finish execute update command for $tableAlias")
    Seq.empty[Row]
  }

  private def buildHoodieConfig(sparkSession: SparkSession): Map[String, String] = {
    val targetTable = sparkSession.sessionState.catalog
      .getTableMetadata(TableIdentifier(tableAlias.identifier, tableAlias.database))
    val path = getTableLocation(targetTable, sparkSession)
      .getOrElse(s"missing location for $tableAlias")

    val primaryColumns = SQLConfig.getPrimaryColumns(targetTable.storage.properties)

    assert(primaryColumns.nonEmpty,
      s"There are no primary key in table $tableAlias, cannot execute update operator")
    withSparkConf(sparkSession, targetTable.storage.properties) {
      Map(
        "path" -> removeStarFromPath(path.toString),
        RECORDKEY_FIELD_OPT_KEY -> primaryColumns.mkString(","),
        KEYGENERATOR_CLASS_OPT_KEY -> classOf[ComplexKeyGenerator].getCanonicalName,
        PRECOMBINE_FIELD_OPT_KEY -> primaryColumns.head, //set the default preCombine field.
        TABLE_NAME -> tableAlias.identifier,
        OPERATION_OPT_KEY -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
        PARTITIONPATH_FIELD_OPT_KEY -> targetTable.partitionColumnNames.mkString(","),
        META_SYNC_ENABLED_OPT_KEY -> "false", // TODO make the meta sync enable by default.
        HIVE_USE_JDBC_OPT_KEY -> "false",
        HIVE_DATABASE_OPT_KEY -> tableAlias.database.getOrElse("default"),
        HIVE_TABLE_OPT_KEY -> tableAlias.identifier,
        HIVE_PARTITION_FIELDS_OPT_KEY -> targetTable.partitionColumnNames.mkString(","),
        HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> classOf[MultiPartKeysValueExtractor].getCanonicalName,
        URL_ENCODE_PARTITIONING_OPT_KEY -> "true",
        INSERT_PARALLELISM -> "64",
        UPSERT_PARALLELISM -> "64"
      )
    }
  }
}
