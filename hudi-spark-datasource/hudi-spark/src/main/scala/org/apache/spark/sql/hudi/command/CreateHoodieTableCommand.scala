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

import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{DEFAULT_PAYLOAD_OPT_VAL, PAYLOAD_CLASS_OPT_KEY}
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.HoodieParquetInputFormat
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.hudi.command.CreateHoodieTableCommand.initTableIfNeed
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Command for create hoodie table.
  * @param table
  * @param ignoreIfExists
  */
case class CreateHoodieTableCommand(table: CatalogTable, ignoreIfExists: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val sessionState = sparkSession.sessionState
    val tableIsExists = sessionState.catalog.tableExists(table.identifier)
    if (tableIsExists) {
      if (ignoreIfExists) {
        // scalastyle:off
        return Seq.empty[Row]
        // scalastyle:on
      } else {
        throw new IllegalArgumentException(s"Table ${table.identifier.unquotedString} already exists.")
      }
    }
    // Add the meta fields to the schema,
    val newSchema = addMetaFields(table.schema)
    var path = getTableLocation(table, sparkSession)
      .getOrElse(s"Missing path for table ${table.identifier}")
    // Append the "*" to the path as currently hoodie must specify
    // the same number of STAR as the partition level.
    // TODO Some command e.g. MSCK REPAIR TABLE  will crash when the path contains "*". And it is
    // also not friendly to users to append the "*" to the path. I have file a issue for this
    // to support no start query for hoodie at https://issues.apache.org/jira/browse/HUDI-1591
    val newPath = if (table.partitionColumnNames.nonEmpty) {
      if (path.endsWith("/")) {
        path = path.substring(0, path.length - 1)
      }
      for (_ <- 0 until table.partitionColumnNames.size + 1) {
        path = s"$path/*"
      }
      path
    } else {
      path
    }
    val tableType = HoodieOptionConfig.getTableType(table.storage.properties)
    val inputFormat = tableType match {
      case DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL =>
        classOf[HoodieParquetInputFormat].getCanonicalName
      case DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL =>
        classOf[HoodieParquetRealtimeInputFormat].getCanonicalName
      case _=> throw new IllegalArgumentException(s"UnKnow table type:$tableType")
    }
    val outputFormat = HoodieInputFormatUtils.getOutputFormatClassName(HoodieFileFormat.PARQUET)
    val serdeFormat = HoodieInputFormatUtils.getSerDeClassName(HoodieFileFormat.PARQUET)

    val newStorage = new CatalogStorageFormat(Some(new Path(newPath).toUri),
      Some(inputFormat), Some(outputFormat), Some(serdeFormat),
      table.storage.compressed, table.storage.properties)

    val newTable = table.copy(schema = newSchema, storage = newStorage)
    sessionState.catalog.createTable(newTable, ignoreIfExists = false)

    initTableIfNeed(sparkSession, table)

    Seq.empty[Row]
  }


}

object CreateHoodieTableCommand extends Logging {

  /**
    * Init the hoodie table if it is not exists.
    * @param sparkSession
    * @param table
    * @return
    */
  def initTableIfNeed(sparkSession: SparkSession, table: CatalogTable): Unit = {
    val location = getTableLocation(table, sparkSession).getOrElse(
      throw new IllegalArgumentException(s"Missing location for ${table.identifier}"))

    val conf = sparkSession.sessionState.newHadoopConf()
    val basePath = new Path(location)
    val fs = basePath.getFileSystem(conf)
    val metaPath = new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME)
    val tableExists = fs.exists(metaPath)
    // Init the hoodie table
    if (!tableExists) {
      val tableName = table.identifier.table
      val parameters = HoodieOptionConfig.mappingSqlOptionToHoodieParam(table.storage.properties)
      val payloadClass = parameters.getOrElse(PAYLOAD_CLASS_OPT_KEY, DEFAULT_PAYLOAD_OPT_VAL)
      val tableType = parameters.getOrElse(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY,
        DataSourceWriteOptions.DEFAULT_TABLE_TYPE_OPT_VAL)

      logInfo(s"Table $tableName is not exists, start to create the hudi table")
      val properties = new Properties()
      properties.putAll(parameters.asJava)
      HoodieTableMetaClient.withPropertyBuilder()
          .fromProperties(properties)
          .setTableName(tableName)
          .setTableType(tableType)
          .setPayloadClassName(payloadClass)
        .initTable(conf, basePath.toString)
    }
  }
}