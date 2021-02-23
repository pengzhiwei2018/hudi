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

package org.apache.hudi

import java.net.URLEncoder

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, GreaterThanOrEqual,
  LessThan, Literal}
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class TestHoodieFileIndex extends HoodieClientTestBase {

  var spark: SparkSession = _
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "timestamp",
    HoodieWriteConfig.TABLE_NAME -> "hoodie_test"
  )

  @BeforeEach override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
    initMetaClient()
  }

  @Test def testPartitionSchema(): Unit = {
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", 100)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1), 2))
    inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING_OPT_KEY, "true")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val fileIndex = HoodieFileIndex(spark, basePath, None, Map("path" -> basePath))
    // If the URL_ENCODE_PARTITIONING_OPT_KEY is enable, the partition columns will store to the
    // hoodie.properties
    assertEquals("partition", fileIndex.partitionSchema.fields.map(_.name).mkString(","))

    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1), 2))
    inputDF2.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING_OPT_KEY, "false")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val fileIndex2 = HoodieFileIndex(spark, basePath, None, Map("path" -> basePath))
    // If the URL_ENCODE_PARTITIONING_OPT_KEY is disable, The partition columns will not store to
    // the hoodie.properties
    assertTrue(fileIndex2.partitionSchema.isEmpty)
  }

  @Test def testPartitionPrune(): Unit = {
    val partitions = Array("2021/03/08", "2021/03/09", "2021/03/10", "2021/03/11", "2021/03/12")
    val newDataGen =  new HoodieTestDataGenerator(partitions)
    val records1 = newDataGen.generateInsertsContainsAllPartitions("000", 100)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1), 2))
    inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING_OPT_KEY, "true")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val fileIndex = HoodieFileIndex(spark, basePath, None, Map("path" -> basePath))

    val partitionFilter1 = EqualTo(attribute("partition"), literal("2021/03/08"))
    metaClient = HoodieTableMetaClient.reload(metaClient)
    metaClient.reloadActiveTimeline()
    val activeInstants = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
    val fileSystemView = new HoodieTableFileSystemView(metaClient, activeInstants)
    val filesIn20210308 = fileSystemView.getAllBaseFiles(URLEncoder.encode("2021/03/08")).iterator().asScala.toSeq
    val partitionAndFilesAfterPrune = fileIndex.listFiles(Seq(partitionFilter1), Seq.empty)
    assertEquals(1, partitionAndFilesAfterPrune.size)

    val PartitionDirectory(partitionValues, filesInPartition) = partitionAndFilesAfterPrune(0)

    assertEquals(partitionValues.toSeq(Seq(StringType)).mkString(","), "2021/03/08")
    assertEquals(filesIn20210308.size, filesInPartition.size)

    val partitionFilter2 = And(
      GreaterThanOrEqual(attribute("partition"), literal("2021/03/08")),
      LessThan(attribute("partition"), literal("2021/03/10"))
    )
    val prunedPartitions = fileIndex.listFiles(Seq(partitionFilter2),
      Seq.empty).map(_.values.toSeq(Seq(StringType)).mkString(",")).toList

    assertEquals(List("2021/03/08", "2021/03/09"), prunedPartitions)
  }

  private def attribute(partition: String): AttributeReference = {
    AttributeReference("partition", StringType, true)()
  }

  private def literal(value: String): Literal = {
    Literal.create(value)
  }
}
