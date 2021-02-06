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

import java.io.File

import scala.collection.JavaConverters._
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField}
import org.apache.spark.util.Utils
import org.scalatest.FunSuite

class HoodieSqlTest extends FunSuite {

  private lazy val spark = SparkSession.builder()
    .master("local[1]")
    .appName("hoodie sql test")
    .withExtensions(new HoodieSparkSessionExtension)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  test("Test Create Hoodie Table") {
    withTempDir { tmp =>
      spark.sql(
        s"""
           |create table h0 (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  primary key (id,name)
           |) using hudi
           | options (
           |  type = 'cow'
           | )
           | location '${tmp.getCanonicalPath}'
       """.stripMargin)
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("h0"))

      assertResult("h0")(table.identifier.table)
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
    }
  }

  test("Test MergeInto") {
    withTempDir { tmp =>
      val tableName = "h1"
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  primary key (id)
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | options (
           |  versionColumn = 'ts',
           |  insertParallelism = 4,
           |  updateParallelism = 4
           | )
       """.stripMargin)

      // first merge (insert a new record)
      spark.sql(
        s"""
           | merge into $tableName
           | using (
           |  select 1 as id, 'a1' as name, 10 as price, 1000 as ts
           | ) s0
           | on s0.id = $tableName.id
           | when matched then update set
           | id = s0.id, name = s0.name, price = s0.price, ts = s0.ts
           | when not matched then insert *
       """.stripMargin)
      val queryResult1 = spark.sql(s"select id, name, price, ts from $tableName").collect()
      assertResult(Array(Row(1, "a1", 10.0, 1000)))(queryResult1)

      // second merge (update the record)
      spark.sql(
        s"""
           | merge into $tableName
           | using (
           |  select 1 as id, 'a1' as name, 10 as price, 1001 as ts
           | ) s0
           | on s0.id = $tableName.id
           | when matched then update set
           | id = s0.id, name = s0.name, price = s0.price + $tableName.price, ts = s0.ts
           | when not matched then insert *
       """.stripMargin)
      val queryResult2 = spark.sql(s"select id, name, price, ts from $tableName").collect()
      assertResult(Array(Row(1, "a1", 20.0, 1001)))(queryResult2)

      // the third time merge (update & insert the record)
      spark.sql(
        s"""
           | merge into $tableName
           | using (
           |  select * from (
           |  select 1 as id, 'a1' as name, 10 as price, 1002 as ts
           |  union all
           |  select 2 as id, 'a2' as name, 12 as price, 1001 as ts
           |  )
           | ) s0
           | on s0.id = $tableName.id
           | when matched then update set
           | id = s0.id, name = s0.name, price = s0.price + $tableName.price, ts = s0.ts
           | when not matched and id % 2 = 0 then insert *
       """.stripMargin)
      val queryResult3 = spark.sql(s"select id, name, price, ts from $tableName").collect()
      assertResult(Array(Row(1, "a1", 30.0, 1002), Row(2, "a2", 12.0, 1001)))(queryResult3)

      // the fourth merge (delete the record)
      spark.sql(
        s"""
           | merge into $tableName
           | using (
           |  select 1 as id, 'a1' as name, 12 as price, 1003 as ts
           | ) s0
           | on s0.id = $tableName.id
           | when matched and id != 1 then update set
           |    id = s0.id, name = s0.name, price = s0.price, ts = s0.ts
           | when matched and id = 1 then delete
           | when not matched then insert *
       """.stripMargin)
      val cnt = spark.sql(s"select * from $tableName where id = 1").count()
      assertResult(0)(cnt)
    }
  }

  test("Test Insert Into") {
    withTempDir { tmp =>
      val tableName = "h2"
      // create a partitioned table
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
           | location '${tmp.getCanonicalPath}'
           | options (
           |  insertParallelism = 4
           | )
       """.stripMargin)
      // insert into dynamic partition
      spark.sql(
        s"""
          | insert into $tableName
          | select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-01-05' as dt
        """.stripMargin)
      val queryResult1 =
        spark.sql(s"select id, name, price, ts, dt from $tableName").collect()
      assertResult(Array(Row(1, "a1", 10.0, 1000, "2021-01-05")))(queryResult1)
      // insert into static partition
      spark.sql(
        s"""
          | insert into $tableName partition(dt = '2021-01-05')
          | select 2 as id, 'a2' as name, 10 as price, 1000 as ts
        """.stripMargin)
      val queryResult2 =
        spark.sql(s"select id, name, price, ts, dt from $tableName where id =2").collect()
      assertResult(Array(Row(2, "a2", 10.0, 1000, "2021-01-05")))(queryResult2)
    }
  }

  test("Test Update Table") {
    withTempDir { tmp =>
      val tableName = "h3"
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  primary key (id)
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | options (
           |  versionColumn = 'ts',
           |  insertParallelism = 4,
           |  updateParallelism = 4
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      val queryResult1 = spark.sql(s"select id, name, price, ts from $tableName").collect()
      assertResult(Array(Row(1, "a1", 10.0, 1000)))(queryResult1)

      // update data
      spark.sql(s"update $tableName set price = 20 where id = 1")
      val queryResult2 = spark.sql(s"select id, name, price, ts from $tableName").collect()
      assertResult(Array(Row(1, "a1", 20.0, 1000)))(queryResult2)

      // update data
      spark.sql(s"update $tableName set price = price * 2 where id = 1")
      val queryResult3 = spark.sql(s"select id, name, price, ts from $tableName").collect()
      assertResult(Array(Row(1, "a1", 40.0, 1000)))(queryResult3)
    }
  }

  test("Test Delete Table") {
    withTempDir { tmp =>
      val tableName = "h4"
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  primary key (id)
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | options (
           |  versionColumn = 'ts',
           |  insertParallelism = 4,
           |  updateParallelism = 4,
           |  deleteParallelism = 4
           | )
       """.stripMargin)
      // insert data to table
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      val queryResult1 = spark.sql(s"select id, name, price, ts from $tableName").collect()
      assertResult(Array(Row(1, "a1", 10.0, 1000)))(queryResult1)

      // delete table
      spark.sql(s"delete from $tableName where id = 1")
      val queryResult2 = spark.sql(s"select id, name, price, ts from $tableName").count()
      assertResult(0)(queryResult2)
    }
  }

  test("Test Create Table As Select") {
    withTempDir { tmp =>
      val tableName = "h5"
      spark.sql(
        s"""
           |create table $tableName using hudi
           | location '${tmp.getCanonicalPath}'
           | options (
           |  versionColumn = 'ts',
           |  insertParallelism = 4,
           |  updateParallelism = 4
           | ) AS
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
       """.stripMargin)
      spark.sql(s"refresh table $tableName")
      val queryResult1 = spark.sql(s"select id, name, price, ts from $tableName").collect()
      assertResult(Array(Row(1, "a1", 10.0, 1000)))(queryResult1)
    }
  }

  private def withTempDir(f: File => Unit): Unit = {
    val tempDir = Utils.createTempDir()
    try f(tempDir) finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}
