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

package org.apache.spark.sql.hudi.command.payload

import java.util.{Base64, Properties}
import java.util.concurrent.Callable

import scala.collection.JavaConverters._
import com.google.common.cache.CacheBuilder
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord, IndexedRecord}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.avro.HoodieAvroUtils.bytesToAvro
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodiePayloadProps, HoodieRecord}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.io.HoodieMergeHandle
import org.apache.hudi.sql.IExpressionEvaluator
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.hudi.SerDeUtils
import org.apache.spark.sql.hudi.command.payload.ExpressionPayload.getEvaluator

import scala.collection.mutable.ArrayBuffer

/**
  * A HoodieRecordPayload for MergeIntoHoodieTableCommand.
  * It will execute the condition and assignments expression in the
  * match and not-match actions and compute the final record to write.
  * @param record
  * @param orderingVal
  */
class ExpressionPayload(record: GenericRecord,
                        orderingVal: Comparable[_])
  extends DefaultHoodieRecordPayload(record, orderingVal) {

  private var  writeSchema: Schema =_

  override def combineAndGetUpdateValue(currentValue: IndexedRecord,
                                        schema: Schema): HOption[IndexedRecord] = {
    throw new IllegalStateException(s"Should not call this method for ${getClass.getCanonicalName}")
  }

  override def getInsertValue(schema: Schema): HOption[IndexedRecord] = {
    throw new IllegalStateException(s"Should not call this method for ${getClass.getCanonicalName}")
  }

  override def combineAndGetUpdateValue(targetRecord: IndexedRecord,
                                        schema: Schema, properties: Properties): HOption[IndexedRecord] = {
    val sourceRecord = bytesToAvro(recordBytes, schema)
    val joinRecord = this.joinRecord(sourceRecord, targetRecord)
    val joinSqlRecord = new SqlTypedRecord(joinRecord)

    // Process update
    val updateConditionAndAssignmentsText = properties.get(HoodiePayloadProps
      .PAYLOAD_UPDATE_CONDITION_AND_ASSIGNMENTS)
    assert(updateConditionAndAssignmentsText != null,
      s"${HoodiePayloadProps.PAYLOAD_UPDATE_CONDITION_AND_ASSIGNMENTS} have not set")

    var resultRecordOpt: HOption[IndexedRecord] = null

    // Get the Evaluator for each condition and update assignments.
    val updateConditionAndAssignments = getEvaluator(updateConditionAndAssignmentsText.toString)
    for ((conditionEvaluator, assignmentEvaluator) <- updateConditionAndAssignments
      if resultRecordOpt == null) {
      val conditionVal = conditionEvaluator.eval(joinSqlRecord).head.asInstanceOf[Boolean]
      // If the update condition matched  then execute assignment expression
      // to compute final record to update. We will return the first matched record.
      if (conditionVal) {
        val results = assignmentEvaluator.eval(joinSqlRecord)
        initWriteSchemaIfNeed(properties)
        val resultRecord = convertToRecord(results, writeSchema)
        // if the PreCombine field value of targetRecord is greate
        // than the new incoming record, just keep the old record value.
        if (noNeedUpdatePersistedRecord(targetRecord, resultRecord, properties)) {
          resultRecordOpt = HOption.of(targetRecord)
        } else {
          resultRecordOpt = HOption.of(resultRecord)
        }
      }
    }
    if (resultRecordOpt == null) {
      // Process delete
      val deleteConditionText = properties.get(HoodiePayloadProps.PAYLOAD_DELETE_CONDITION)
      if (deleteConditionText != null) {
        val deleteCondition = getEvaluator(deleteConditionText.toString).head._1
        val deleteConditionVal = deleteCondition.eval(joinRecord).head.asInstanceOf[Boolean]
        if (deleteConditionVal) {
          resultRecordOpt = HOption.empty()
        }
      }
    }
    if (resultRecordOpt == null) {
      // If there is no condition matched, just filter this record.
      // here we return a IGNORE_RECORD, HoodieMergeHandle will not handle it.
      HOption.of(HoodieMergeHandle.IGNORE_RECORD)
    } else {
      resultRecordOpt
    }
  }

  override def getInsertValue(schema: Schema, properties: Properties): HOption[IndexedRecord] = {
    val incomingRecord = bytesToAvro(recordBytes, schema)
    if (isDeleteRecord(incomingRecord)) {
       HOption.empty[IndexedRecord]()
    } else {
      val insertConditionAndAssignmentsText =
        properties.get(HoodiePayloadProps.PAYLOAD_INSERT_CONDITION_AND_ASSIGNMENTS)
      val sqlTypedRecord = new SqlTypedRecord(incomingRecord)
      // Get the evaluator for each condition and insert assignment.
      val insertConditionAndAssignments =
        ExpressionPayload.getEvaluator(insertConditionAndAssignmentsText.toString)
      var resultRecordOpt: HOption[IndexedRecord] = null
      for ((conditionEvaluator, assignmentEvaluator) <- insertConditionAndAssignments
           if resultRecordOpt == null) {
        val conditionVal = conditionEvaluator.eval(sqlTypedRecord).head.asInstanceOf[Boolean]
        // If matched the insert condition then execute the assignment expressions to compute the
        // result record. We will return the first matched record.
        if (conditionVal) {
          val results = assignmentEvaluator.eval(sqlTypedRecord)
          initWriteSchemaIfNeed(properties)
          resultRecordOpt = HOption.of(convertToRecord(results, writeSchema))
        }
      }
      if (resultRecordOpt != null) {
        resultRecordOpt
      } else {
        // If there is no condition matched, just filter this record.
        // Here we return a IGNORE_RECORD, HoodieCreateHandle will not handle it.
        HOption.of(HoodieMergeHandle.IGNORE_RECORD)
      }
    }
  }

  private def convertToRecord(values: Array[AnyRef], schema: Schema): IndexedRecord = {
    assert(values.length == schema.getFields.size())
    val writeRecord = new GenericData.Record(schema)
    for (i <- values.indices) {
      writeRecord.put(i, values(i))
    }
    writeRecord
  }

  /**
    * Init the write schema.
    * @param properties
    */
  private def initWriteSchemaIfNeed(properties: Properties): Unit = {
    if (writeSchema == null) {
      assert(properties.containsKey(HoodieWriteConfig.WRITE_SCHEMA),
        s"Missing ${HoodieWriteConfig.WRITE_SCHEMA}")
      writeSchema = new Schema.Parser()
        .parse(properties.getProperty(HoodieWriteConfig.WRITE_SCHEMA))
    }
  }

  /**
    * Join the source record with the target record.
    * @param sourceRecord
    * @param targetRecord
    * @return
    */
  private def joinRecord(sourceRecord: IndexedRecord, targetRecord: IndexedRecord): IndexedRecord = {
    val leftSchema = sourceRecord.getSchema
    // the targetRecord is load from the disk, it contains the meta fields, so we remove it here
    val rightSchema = HoodieAvroUtils.removeMetadataFields(targetRecord.getSchema)
    val joinSchema = mergeSchema(leftSchema, rightSchema)

    val values = new ArrayBuffer[AnyRef]()
    for (i <- 0 until joinSchema.getFields.size()) {
      val value = if (i < leftSchema.getFields.size()) {
        sourceRecord.get(i)
      } else { // skip meta field
        targetRecord.get(i - leftSchema.getFields.size() + HoodieRecord.HOODIE_META_COLUMNS.size())
      }
      values += value
    }
    convertToRecord(values.toArray, joinSchema)
  }

  def mergeSchema(a: Schema, b: Schema): Schema = {
    val mergedFields =
      a.getFields.asScala.map(field =>
        new Schema.Field("a_" + field.name,
          field.schema, field.doc, field.defaultVal, field.order)) ++
      b.getFields.asScala.map(field =>
        new Schema.Field("b_" + field.name,
          field.schema, field.doc, field.defaultVal, field.order))
    Schema.createRecord(a.getName, a.getDoc, a.getNamespace, a.isError, mergedFields.asJava)
  }
}

object ExpressionPayload {

  private val cache = CacheBuilder.newBuilder()
    .maximumSize(1024)
    .build[String, Map[IExpressionEvaluator, IExpressionEvaluator]]()

  /**
    * Do the CodeGen for each condition and assignment expressions.We will cache it to reduce
    * the compile time for each method call.
    * @param serializedConditionAssignments
    * @return
    */
  def getEvaluator(
    serializedConditionAssignments: String): Map[IExpressionEvaluator, IExpressionEvaluator] = {
    cache.get(serializedConditionAssignments,
      new Callable[Map[IExpressionEvaluator, IExpressionEvaluator]] {

        override def call(): Map[IExpressionEvaluator, IExpressionEvaluator] = {
          val serializedBytes = Base64.getDecoder.decode(serializedConditionAssignments)
          val conditionAssignments = SerDeUtils.toObject(serializedBytes)
            .asInstanceOf[Map[Expression, Seq[Assignment]]]
          // Do the CodeGen for condition expression and assignment expression
          conditionAssignments.map {
            case (condition, assignments) =>
              val conditionEvaluator = ExpressionCodeGen.doCodeGen(Seq(condition))
              val assignmentEvaluator = ExpressionCodeGen.doCodeGen(assignments)
              conditionEvaluator -> assignmentEvaluator
          }
        }
      })
  }
}

