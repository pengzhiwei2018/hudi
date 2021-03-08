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

import scala.collection.JavaConverters._
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.exception.HoodieDuplicateKeyException
import org.apache.hudi.hadoop.HoodieParquetInputFormat
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField}

class CreateTableTest extends HoodieBaseSqlTest {

  test("Test Create Hoodie Table") {
    withTempDir { tmp =>
      // Test create cow table.
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | options (
           |  primaryKey = 'id,name',
           |  type = 'cow'
           | )
           | location '${tmp.getCanonicalPath}'
       """.stripMargin)
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))

      assertResult(tableName)(table.identifier.table)
      assertResult("hudi")(table.provider.get)
      assertResult(CatalogTableType.EXTERNAL)(table.tableType)
      assertResult(
        HoodieRecord.HOODIE_META_COLUMNS.asScala.map(StructField(_, StringType))
          ++ Seq(
          StructField("id", IntegerType),
          StructField("name", StringType),
          StructField("price", DoubleType),
          StructField("ts", LongType))
      )(table.schema.fields)
      assertResult(Map("type" -> "cow", "primaryKey" -> "id,name"))(table.storage.properties)
      assertResult(classOf[HoodieParquetInputFormat].getCanonicalName)(table.storage.inputFormat.get)

      spark.sql(s"drop table $tableName")
      // Test create mor partitioned table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           |) using hudi
           | partitioned by (dt)
           | options (
           |  primaryKey = 'id',
           |  type = 'mor'
           | )
           | location '${tmp.getCanonicalPath}/h0'
       """.stripMargin)
      val table2 = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      assertResult(Map("type" -> "mor", "primaryKey" -> "id"))(table2.storage.properties)
      assertResult(Seq("dt"))(table2.partitionColumnNames)
      assertResult(classOf[HoodieParquetRealtimeInputFormat].getCanonicalName)(table2.storage.inputFormat.get)
    }
  }

  test("Test Create Table As Select") {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName using hudi
           | location '${tmp.getCanonicalPath}'
           | AS
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
       """.stripMargin)
      val queryResult1 = spark.sql(s"select id, name, price, ts from $tableName").collect()
      assertResult(Array(Row(1, "a1", 10.0, 1000)))(queryResult1)
    }
  }
}
