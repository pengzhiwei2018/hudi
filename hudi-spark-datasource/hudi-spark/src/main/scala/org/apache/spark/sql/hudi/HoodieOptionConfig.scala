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

package org.apache.spark.sql.hudi

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig

/**
  * The HoodieOptionConfig defines some short name for the hoodie
  * option key and value to simplify the use cost.
  */
object HoodieOptionConfig {
  /**
    * The short name for the record row key.
    */
  val SQL_KEY_TABLE_PRIMARY_KEY = "primaryKey"

  /**
    * The short name for the table type.
    */
  val SQL_KEY_TABLE_TYPE = "type"

  /**
    * The short name for the pre-combine field.
    */
  val SQL_KEY_VERSION_COLUMN = "versionColumn"

  /**
    * The short name for the insert parallelism.
    */
  val SQL_KEY_INSERT_PARALLELISM = "insertParallelism"

  /**
    * The short name for the update parallelism.
    */
  val SQL_KEY_UPDATE_PARALLELISM = "updateParallelism"

  /**
    * The short name for the delete parallelism.
    */
  val SQL_KEY_DELETE_PARALLELISM = "deleteParallelism"

  /**
    * The short name for INSERT_DROP_DUPS_OPT_KEY.
    */
  val SQL_KEY_DROP_INSERT_DUPLCATE_KEY = "dropDup"

  /**
    * The mapping of the sql short name key to the hoodie's config key.
    */
  private val keyMapping: Map[String, String] = Map (
    SQL_KEY_TABLE_PRIMARY_KEY -> DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY,
    SQL_KEY_TABLE_TYPE -> DataSourceWriteOptions.TABLE_TYPE_OPT_KEY,
    SQL_KEY_VERSION_COLUMN -> DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY,
    SQL_KEY_INSERT_PARALLELISM -> HoodieWriteConfig.INSERT_PARALLELISM,
    SQL_KEY_UPDATE_PARALLELISM -> HoodieWriteConfig.UPSERT_PARALLELISM,
    SQL_KEY_DELETE_PARALLELISM -> HoodieWriteConfig.DELETE_PARALLELISM,
    SQL_KEY_DROP_INSERT_DUPLCATE_KEY -> DataSourceWriteOptions.INSERT_DROP_DUPS_OPT_KEY
  ).mapValues(withPrefix)

  /**
    * The short name for the value of COW_TABLE_TYPE_OPT_VAL.
    */
  val SQL_VALUE_TABLE_TYPE_COW = "cow"

  /**
    * The short name for the value of MOR_TABLE_TYPE_OPT_VAL.
    */
  val SQL_VALUE_TABLE_TYPE_MOR = "mor"

  private val valueMapping: Map[String, String] = Map (
    SQL_VALUE_TABLE_TYPE_COW -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
    SQL_VALUE_TABLE_TYPE_MOR -> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL
  )

  /**
    * Mapping the sql's short name key/value in the options to the hoodie's config key/value.
    * @param options
    * @return
    */
  def mappingSqlOptionToHoodieParam(options: Map[String, String]): Map[String, String] = {
    options.map (kv =>
      keyMapping.getOrElse(kv._1, kv._1) -> valueMapping.getOrElse(kv._2, kv._2))
  }

  /**
    * Get the primary key from the table options.
    * @param options
    * @return
    */
  def getPrimaryColumns(options: Map[String, String]): Array[String] = {
    val params = mappingSqlOptionToHoodieParam(options)
    params.get(withPrefix(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY))
      .map(_.split(",").filter(_.length > 0))
      .getOrElse(Array.empty)
  }

  /**
    * Get the table type from the table options.
    * @param options
    * @return
    */
  def getTableType(options: Map[String, String]): String = {
    val params = mappingSqlOptionToHoodieParam(options)
    params.getOrElse(withPrefix(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY),
      DataSourceWriteOptions.DEFAULT_TABLE_TYPE_OPT_VAL)
  }

  /**
    * Prefix for the spark config for hoodie key.
    * For example "spark.hoodie.datasource.hive_sync.enable" in spark config is
    * mapping to the hoodie config key "hoodie.datasource.hive_sync.enable".
    */
  val CONFIG_PREFIX = "spark."

  def withPrefix(key: String): String = s"$CONFIG_PREFIX$key"

  def tripPrefix(keyWithPrefix: String): String = {
    if (keyWithPrefix.startsWith(CONFIG_PREFIX)) {
      keyWithPrefix.substring(CONFIG_PREFIX.length)
    } else {
      keyWithPrefix
    }
  }
}
