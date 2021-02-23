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

import scala.collection.JavaConverters._
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory, PartitionUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * A File Index which support partition prune for hoodie snapshot and read-optimized
  * query.
  * Main steps to get the file list for query:
  * 1、Load all files and partition values from the table path.
  * 2、Do the partition prune by the partition filter condition.
  *
  * Note:
  * Only when the URL_ENCODE_PARTITIONING_OPT_KEY is enable, we can store the partition columns
  * to the hoodie.properties in HoodieSqlWriter when write table. So that the query can benefit
  * from the partition prune.
  */
case class HoodieFileIndex(
     spark: SparkSession,
     basePath: String,
     schemaSpec: Option[StructType],
     options: Map[String, String])
  extends FileIndex with Logging {

  private val hadoopConf = spark.sessionState.newHadoopConf()
  private val fs = new Path(basePath).getFileSystem(hadoopConf)
  private lazy val metaClient = HoodieTableMetaClient
    .builder().setConf(hadoopConf).setBasePath(basePath).build()

  private val queryPath = new Path(options.getOrElse("path", "'path' option required"))
  /**
    * Get the schema of the table.
    */
  lazy val schema: StructType = schemaSpec.getOrElse({
    val schemaUtil = new TableSchemaResolver(metaClient)
    SchemaConverters.toSqlType(schemaUtil.getTableAvroSchema)
      .dataType.asInstanceOf[StructType]
  })

  /**
    * Get the partition schema.
    */
  private lazy val _partitionSchema: StructType = {
    val tableConfig = metaClient.getTableConfig
    val partitionColumns = tableConfig.getPartitionColumns
    val nameFieldMap = schema.fields.map(filed => filed.name -> filed).toMap
    // If the URL_ENCODE_PARTITIONING_OPT_KEY has enable, the partition schema will stored in
    // hoodie.properties, So we can benefit from the partition prune.
    if (partitionColumns.isPresent) {
      val partitionFields = partitionColumns.get().map(column =>
        nameFieldMap.getOrElse(column, throw new IllegalArgumentException(s"Cannot find column " +
          s"$column in the schema[${schema.fields.mkString(",")}]")))
      new StructType(partitionFields)
    } else { // If the URL_ENCODE_PARTITIONING_OPT_KEY is disable, we trait it as a
      // none-partitioned table.
      new StructType()
    }
  }

  private val timeZoneId = CaseInsensitiveMap(options)
    .get(DateTimeUtils.TIMEZONE_OPTION)
    .getOrElse(SQLConf.get.sessionLocalTimeZone)

  @volatile private var fileSystemView: HoodieTableFileSystemView = _
  @volatile private var cachedAllInputFiles: Array[HoodieBaseFile] = _
  @volatile private var cachedFileSize: Long = 0L
  @volatile private var cachedAllPartitionPaths: Seq[PartitionPath] = _

  refresh()

  override def rootPaths: Seq[Path] = queryPath :: Nil

  override def listFiles(partitionFilters: Seq[Expression],
                         dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    if (partitionSchema.fields.isEmpty) { // None partition table.
      val allFiles = cachedAllInputFiles.map(_.getFileStatus)
      Seq(PartitionDirectory(InternalRow.empty, allFiles))
    } else {
      // Prune the partition path by the partition filters
      val prunedPartitions = prunePartition(cachedAllPartitionPaths, partitionFilters)
      prunedPartitions.map { partition =>
        val fileStatues = fileSystemView.getLatestBaseFiles(partition.partitionPath).iterator()
          .asScala.toSeq
          .map(_.getFileStatus)
        PartitionDirectory(partition.values, fileStatues)
      }
    }
  }

  override def inputFiles: Array[String] = {
    cachedAllInputFiles.map(_.getFileStatus.getPath.toString)
  }

  override def refresh(): Unit = {
    val startTime = System.currentTimeMillis()
    val partitionFiles = loadPartitionPathFiles(queryPath, fs)
    val allFiles = if (partitionFiles.isEmpty) Array.empty[FileStatus]
    else partitionFiles.values.reduce(_ ++ _).toArray

    metaClient.reloadActiveTimeline()
    val activeInstants = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
    fileSystemView = new HoodieTableFileSystemView(metaClient, activeInstants, allFiles)
    cachedAllInputFiles = fileSystemView.getLatestBaseFiles.iterator().asScala.toArray
    cachedAllPartitionPaths = partitionFiles.keys.toSeq
    cachedFileSize = cachedAllInputFiles.map(_.getFileLen).sum

    val flushSpend = System.currentTimeMillis() - startTime
    logInfo(s"Refresh for table ${metaClient.getTableConfig.getTableName}," +
      s" spend: $flushSpend ms")
  }

  override def sizeInBytes: Long = {
    cachedFileSize
  }

  override def partitionSchema: StructType = _partitionSchema

  /**
    * Get the data schema of the table.
    * @return
    */
  def dataSchema: StructType = {
    val partitionColumns = _partitionSchema.fields.map(_.name).toSet
    StructType(schema.fields.filterNot(f => partitionColumns.contains(f.name)))
  }

  def allFiles: Seq[FileStatus] = cachedAllInputFiles.map(_.getFileStatus)

  private def prunePartition(partitionPaths: Seq[PartitionPath],
                             predicates: Seq[Expression]): Seq[PartitionPath] = {

    val partitionColumnNames = partitionSchema.fields.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }
    if (partitionPruningPredicates.nonEmpty) {
      val predicate = partitionPruningPredicates.reduce(expressions.And)

      val boundPredicate = InterpretedPredicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionSchema.indexWhere(a.name == _.name)
          BoundReference(index, partitionSchema(index).dataType, nullable = true)
      })

      val selected = partitionPaths.filter {
        case PartitionPath(values, _) => boundPredicate.eval(values)
      }
      logInfo {
        val total = partitionPaths.length
        val selectedSize = selected.length
        val percentPruned = (1 - selectedSize.toDouble / total.toDouble) * 100
        s"Selected $selectedSize partitions out of $total, " +
          s"pruned ${if (total == 0) "0" else s"$percentPruned%"} partitions."
      }
      selected
    } else {
      partitionPaths
    }
  }

  /**
    * Load all partition paths and it's files under the specify directory.
    * @param dir The specify directory to load.
    * @param fs  FileSystem
    * @return A Map of PartitionPath and files in the PartitionPath under the "dir" directory.
    */
  private def loadPartitionPathFiles(dir: Path, fs: FileSystem): Map[PartitionPath, Seq[FileStatus]] = {
    val subFiles = fs.listStatus(dir).filterNot(_.getPath.getName.startsWith("."))
    if (subFiles.isEmpty) {
      Map.empty
    } else {
      val (dirs, files) = subFiles.partition(_.isDirectory)
      val partitionFiles = mutable.Map[PartitionPath, ListBuffer[FileStatus]]()
      if (files.nonEmpty) {
        files.foreach { file =>
          val partitionPathString = FSUtils.getRelativePartitionPath(new Path(basePath),
            file.getPath.getParent)

          val partitionValues = if (_partitionSchema.fields.isEmpty) { // None partitioned table
            InternalRow.empty
          } else {
            val partitionSeqs = partitionPathString.split("/")
            assert(partitionSeqs.length == _partitionSchema.size,
              s"size of partition values[size is: ${partitionSeqs.size}, path " +
                s"is: '$partitionPathString'] is not equal to the size of" +
                s" partition schema field[size is: ${_partitionSchema.size}," +
                s" fields is: ${_partitionSchema.json}].")
            // Append partition name to the partition value.
            // e.g. convert "/xx/xx/2021/02" to "/xx/xx/year=2021/month=02"
            val partitionWithName =
              partitionSeqs.zip(_partitionSchema).map {
                case (partition, field) =>
                  if (partition.indexOf("=") == -1) {
                    s"${field.name}=$partition"
                  } else {
                    partition
                  }
              }.mkString("/")
            val pathWithPartitionName = new Path(basePath, partitionWithName)
            val partitionDataTypes = _partitionSchema.fields.map(f => f.name -> f.dataType).toMap
            val partitionValues = PartitionUtils.parsePartition(pathWithPartitionName,
              typeInference = false, Set(new Path(basePath)), partitionDataTypes,
              DateTimeUtils.getTimeZone(timeZoneId))
            // Convert partitionValues to InternalRow
            partitionValues.map(_.literals.map(_.value))
              .map(InternalRow.fromSeq)
              .getOrElse(InternalRow.empty)
          }
          val partitionPath = PartitionPath(partitionValues, partitionPathString)
          if (!partitionFiles.contains(partitionPath)) {
            partitionFiles.put(partitionPath, new ListBuffer[FileStatus])
          }
          partitionFiles(partitionPath).append(file)
        }
      }
      if (dirs.nonEmpty) {
        dirs.map(s => loadPartitionPathFiles(s.getPath, fs))
          .foreach(map => {
            map.foreach { kv =>
              if (!partitionFiles.contains(kv._1)) {
                partitionFiles.put(kv._1, new ListBuffer[FileStatus])
              }
              partitionFiles(kv._1).appendAll(kv._2)
            }
          })
      }
      partitionFiles.toMap
    }
  }

  /**
    * Represent a partition path.
    * e.g. PartitionPath(InternalRow("2021","02","01"), "2021/02/01"))
    * @param values The partition values of this partition path.
    * @param partitionPath The partition path string.
    */
  case class PartitionPath(values: InternalRow, partitionPath: String) {
    override def equals(other: Any): Boolean = other match {
      case PartitionPath(_, otherPath) => partitionPath == otherPath
      case _ => false
    }

    override def hashCode(): Int = {
      partitionPath.hashCode
    }
  }
}
