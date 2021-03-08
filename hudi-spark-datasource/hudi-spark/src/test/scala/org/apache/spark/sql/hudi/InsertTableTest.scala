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

import org.apache.hudi.exception.HoodieDuplicateKeyException
import org.apache.spark.sql.Row

class InsertTableTest extends HoodieBaseSqlTest {
  test("Test Insert Into") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create a partitioned table
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
       """.stripMargin)
      // Insert into dynamic partition
      spark.sql(
        s"""
           | insert into $tableName
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-01-05' as dt
        """.stripMargin)
      val queryResult1 =
        spark.sql(s"select id, name, price, ts, dt from $tableName").collect()
      assertResult(Array(Row(1, "a1", 10.0, 1000, "2021-01-05")))(queryResult1)
      // Insert into static partition
      spark.sql(
        s"""
           | insert into $tableName partition(dt = '2021-01-05')
           | select 2 as id, 'a2' as name, 10 as price, 1000 as ts
        """.stripMargin)
      val queryResult2 =
        spark.sql(s"select id, name, price, ts, dt from $tableName").collect()
      assertResult(Array(
        Row(1, "a1", 10.0, 1000, "2021-01-05"),
        Row(2, "a2", 10.0, 1000, "2021-01-05")))(queryResult2)
    }
  }

  test("Test Insert Into None Partitioned Table") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create none partitioned MOR table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | options (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  versionColumn = 'ts'
           | )
       """.stripMargin)

      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
      val queryResult1 =
        spark.sql(s"select id, name, price, ts from $tableName").collect()
      assertResult(Array(Row(1, "a1", 10.0, 1000)))(queryResult1)

      spark.sql(s"insert into $tableName select 2, 'a2', 12, 1000")
      val queryResult2 =
        spark.sql(s"select id, name, price, ts from $tableName").collect()
      assertResult(Array(Row(1, "a1", 10.0, 1000), Row(2, "a2", 12.0, 1000)))(queryResult2)

      assertThrows[HoodieDuplicateKeyException] {
        try {
          spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")
        } catch {
          case e: Exception =>
            var root: Throwable = e
            while (root.getCause != null) {
              root = root.getCause
            }
            throw root
        }
      }
      // Create table with dropDup is true
      val tableName2 = generateTableName
      spark.sql(
        s"""
           |create table $tableName2 (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName2'
           | options (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  versionColumn = 'ts',
           |  dropDup = true
           | )
       """.stripMargin)
      spark.sql(s"insert into $tableName2 select 1, 'a1', 10, 1000")
      // This record will be drop when dropDup is true
      spark.sql(s"insert into $tableName2 select 1, 'a1', 12, 1000")

      val queryResult3 =
        spark.sql(s"select id, name, price, ts from $tableName2").collect()
      assertResult(Array(Row(1, "a1", 10.0, 1000)))(queryResult3)
    }
  }

  test("Test Insert Overwrite") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create a partitioned table
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
           | location '${tmp.getCanonicalPath}/$tableName'
       """.stripMargin)

     //  Insert overwrite dynamic partition
      spark.sql(
        s"""
           | insert overwrite table $tableName
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-01-05' as dt
        """.stripMargin)
      val queryResult1 =
        spark.sql(s"select id, name, price, ts, dt from $tableName").collect()
      assertResult(Array(Row(1, "a1", 10.0, 1000, "2021-01-05")))(queryResult1)

      // Insert overwrite static partition
      spark.sql(
        s"""
           | insert overwrite table $tableName partition(dt = '2021-01-05')
           | select 2 , 'a2', 10, 1000
        """.stripMargin)
      val queryResult2 =
        spark.sql(s"select id, name, price, ts, dt from $tableName").collect()
      assertResult(Array(Row(2, "a2", 10.0, 1000, "2021-01-05")))(queryResult2)

      // Insert data from another table
      spark.sql(
        s"""
           | create table s0 (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           | ) using parquet
           | location '${tmp.getCanonicalPath}/s0'
         """.stripMargin)
      spark.sql("insert into s0 select 1, 'a1', 10, 1000")
      spark.sql(
        s"""
           | insert overwrite table $tableName partition(dt ='2021-03-05')
           | select * from s0
        """.stripMargin)
      val queryResult3 =
        spark.sql(s"select id, name, price, ts, dt from $tableName").collect()
      assertResult(Array(Row(1, "a1", 10.0, 1000, "2021-03-05")))(queryResult3)
    }
  }
}
