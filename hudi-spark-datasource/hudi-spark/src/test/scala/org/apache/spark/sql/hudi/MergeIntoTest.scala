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

import org.apache.spark.sql.Row

class MergeIntoTest extends HoodieBaseSqlTest {

  test("Test MergeInto") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}'
           | options (
           |  primaryKey ='id',
           |  versionColumn = 'ts'
           | )
       """.stripMargin)

      // First merge (insert a new record)
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

  test("Test MergeInto with multi matches") {
    withTempDir {tmp =>
      val sourceTable = generateTableName
      val targetTable = generateTableName
      // Create source table
      spark.sql(
        s"""
           | create table $sourceTable (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           | ) using parquet
           | location '${tmp.getCanonicalPath}/$sourceTable'
         """.stripMargin)
      // Create target table
      spark.sql(
        s"""
           |create table $targetTable (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$targetTable'
           | options (
           |  primaryKey ='id',
           |  versionColumn = 'ts'
           | )
       """.stripMargin)
      // Insert data to source table
      spark.sql(s"insert into $sourceTable values(1, 'a1', '10', 1000)")
      spark.sql(s"insert into $sourceTable values(2, 'a2', '11', 1000)")

      spark.sql(
        s"""
           | merge into $targetTable as t0
           | using $sourceTable as s0
           | on t0.id = s0.id
           | when matched then update set *
           | when not matched and name = 'a1' then insert *
         """.stripMargin)
      // The record of "name = 'a2'" will be filter
      val queryResult1 = spark.sql(s"select id, name, price, ts from $targetTable").collect()
      assertResult(Array(Row(1, "a1", 10.0, 1000)))(queryResult1)

      spark.sql(s"insert into $targetTable select 3, 'a3', 12, 1000")
      val queryResult2 = spark.sql(s"select id, name, price, ts from $targetTable").collect()
      assertResult(Array(Row(1, "a1", 10.0, 1000), Row(3, "a3", 12, 1000)))(queryResult2)

      spark.sql(
        s"""
           | merge into $targetTable as t0
           | using (
           |  select * from (
           |    select 1 as id, 'a1' as name, 20 as price, 1001 as ts
           |    union all
           |    select 3 as id, 'a3' as name, 20 as price, 1001 as ts
           |    union all
           |    select 4 as id, 'a4' as name, 10 as price, 1001 as ts
           |  )
           | ) s0
           | on s0.id = t0.id
           | when matched and id = 1 then update set id = s0.id, name = t0.name, price =
           | s0.price, ts = s0.ts
           | when matched and id = 3 then update set id = s0.id, name = t0.name, price =
           | t0.price, ts = s0.ts
         """.stripMargin
      )
      val queryResult3 = spark.sql(s"select id, name, price, ts from $targetTable").collect()
      assertResult(Array(Row(1, "a1", 20.0, 1001), Row(3, "a3", 12.0, 1001)))(queryResult3)
    }
  }
}
