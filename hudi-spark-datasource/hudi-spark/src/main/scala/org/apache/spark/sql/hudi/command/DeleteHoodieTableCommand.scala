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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{KEYGENERATOR_CLASS_OPT_KEY, OPERATION_OPT_KEY, PARTITIONPATH_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.config.HoodieWriteConfig.{DELETE_PARALLELISM, TABLE_NAME}
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.{Column, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.hudi.SQLConfig
import org.apache.spark.sql.hudi.logical.DeleteTable

case class DeleteHoodieTableCommand(deleteTable: DeleteTable) extends RunnableCommand {

  private val table = deleteTable.table

  private val tableAlias = table match {
    case SubqueryAlias(name, _) => name
    case _ => throw new IllegalArgumentException(s"Illegal table: $table")
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logInfo(s"start execute delete command for $tableAlias")

    var df = Dataset.ofRows(sparkSession, table)
    if (deleteTable.condition.isDefined) {
      df = df.filter(Column(deleteTable.condition.get))
    }
    val config = buildHoodieConfig(sparkSession)
    df.write
      .format("hudi")
      .mode(SaveMode.Append)
      .options(config)
      .save()
    table.refresh()
    logInfo(s"finish execute delete command for $tableAlias")
    Seq.empty[Row]
  }

  private def buildHoodieConfig(sparkSession: SparkSession): Map[String, String] = {
    val targetTable = sparkSession.sessionState.catalog
      .getTableMetadata(TableIdentifier(tableAlias.identifier, tableAlias.database))
    val path = getTableLocation(targetTable, sparkSession)
      .getOrElse(s"missing location for $tableAlias")

    val primaryColumns = SQLConfig.getPrimaryColumns(targetTable.storage.properties)

    assert(primaryColumns.nonEmpty,
      s"There are no primary key in table $tableAlias, cannot execute delete operator")

    withSparkConf(sparkSession, targetTable.storage.properties) {
      Map(
        "path" -> removeStarFromPath(path.toString),
        RECORDKEY_FIELD_OPT_KEY -> primaryColumns.mkString(","),
        KEYGENERATOR_CLASS_OPT_KEY -> classOf[ComplexKeyGenerator].getCanonicalName,
        TABLE_NAME -> tableAlias.identifier,
        OPERATION_OPT_KEY -> DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL,
        PARTITIONPATH_FIELD_OPT_KEY -> targetTable.partitionColumnNames.mkString(","),
        DELETE_PARALLELISM -> "64"
      )
    }
  }
}
