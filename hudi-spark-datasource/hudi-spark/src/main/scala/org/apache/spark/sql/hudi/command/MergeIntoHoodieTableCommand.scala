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

import java.util.Base64

import org.apache.avro.Schema
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.HoodiePayloadProps._
import org.apache.hudi.config.HoodieWriteConfig.{DELETE_PARALLELISM, INSERT_PARALLELISM, TABLE_NAME, UPSERT_PARALLELISM}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.hudi.{AvroConversionUtils, HoodieSparkSqlWriter, HoodieWriterUtils}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BoundReference, Cast, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, MergeIntoTable, SubqueryAlias, UpdateAction}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.types.{BooleanType, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.hudi.{HoodieOptionConfig, SerDeUtils}
import org.apache.spark.sql.hudi.command.payload.ExpressionPayload

/**
  * The Command for hoodie MergeIntoTable.
  * The match on condition must contain the row key fields currently, so that we can use Hoodie
  * Index to speed up the performance.
  *
  * We pushed down all the matched and not matched (condition, assignment) expression pairs to the
  * ExpressionPayload. And the matched (condition, assignment) expression pairs will execute in the
  * ExpressionPayload#combineAndGetUpdateValue to compute the result record, while the not matched
  * expression pairs will execute in the ExpressionPayload#getInsertValue.
  *
  * @param mergeInto
  */
case class MergeIntoHoodieTableCommand(mergeInto: MergeIntoTable) extends RunnableCommand {

  private var sparkSession: SparkSession = _

  /**
    * The alias name of the source.
    */
  private lazy val sourceAlias: String = mergeInto.sourceTable match {
    case SubqueryAlias(name, _) => name.unquotedString
    case plan => throw new IllegalArgumentException(s"Illegal plan $plan in source")
  }

  /**
    * The alias name of the target.
    */
  private lazy val targetAlias: String = mergeInto.targetTable match {
    case SubqueryAlias(name, _) => name.unquotedString
    case plan => throw new IllegalArgumentException(s"Illegal plan $plan in target")
  }

  /**
    * The target table identify.
    */
  private lazy val targetTableIdentify: TableIdentifier = {
    val aliaId = mergeInto.targetTable match {
      case SubqueryAlias(_, SubqueryAlias(tableId, _)) => tableId
      case SubqueryAlias(tableId, _) => tableId
      case plan => throw new IllegalArgumentException(s"Illegal plan $plan in target")
    }
    TableIdentifier(aliaId.identifier, aliaId.database)
  }

  /**
    * The target table schema without hoodie meta fields.
    */
  private lazy val targetTableSchemaWithoutMetaFields =
    removeMetaFields(mergeInto.targetTable.schema).fields

  private lazy val targetTable =
    sparkSession.sessionState.catalog.getTableMetadata(targetTableIdentify)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    this.sparkSession = sparkSession
    // Create the default parameters
    val parameters = buildMergeIntoConfig(mergeInto)
    val sourceDf = Dataset.ofRows(sparkSession, mergeInto.sourceTable)

    if (mergeInto.matchedActions.nonEmpty) { // Do the upsert
      executeUpsert(sourceDf, parameters)
    } else { // If there is no match actions in the statement, execute insert operation only.
      executeInsertOnly(sourceDf, parameters)
    }
    sparkSession.catalog.refreshTable(targetTableIdentify.unquotedString)
    Seq.empty[Row]
  }

  /**
    * Execute the update and delete action. All the matched and not-matched actions will
    * execute in one upsert write operation. We pushed down the matched condition and assignment
    * expressions to the ExpressionPayload#combineAndGetUpdateValue and the not matched
    * expressions to the ExpressionPayload#getInsertValue.
    * @param sourceDf
    * @param parameters
    */
  private def executeUpsert(sourceDf: DataFrame, parameters: Map[String, String]): Unit = {
    val updateActions = mergeInto.matchedActions.filter(_.isInstanceOf[UpdateAction])
      .map(_.asInstanceOf[UpdateAction])
    // Check for the update actions
    checkUpdateAssignments(updateActions)

    val deleteActions = mergeInto.matchedActions.filter(_.isInstanceOf[DeleteAction])
      .map(_.asInstanceOf[DeleteAction])
    assert(deleteActions.size <= 1, "Should be only on delete action in the merge into statement.")
    val deleteAction = deleteActions.headOption

    val insertActions = mergeInto.notMatchedActions.map(_.asInstanceOf[InsertAction])
    // Check for the insert actions
    checkInsertAssignments(insertActions)

    // Append the write schema to the parameters. In the case of merge into, the schema of sourceDf
    // may be different from the target table, because the are transform logical in the update or
    // insert actions. So we must specify the actual schema to the WRITE_SCHEMA. For more
    // information about this, see the comment on HoodieWriteConfig#getWriteSchema.
    var writeParams = parameters +
      (OPERATION_OPT_KEY -> UPSERT_OPERATION_OPT_VAL) +
       (HoodieWriteConfig.WRITE_SCHEMA -> getWriteSchema.toString)

    // Map of Condition -> Assignments
    val updateConditionToAssignments =
      updateActions.map(update => {
        val rewriteCondition = update.condition.map(replaceAttributeInExpression(_, sourceDf))
          .getOrElse(Literal.create(true, BooleanType))
        val formatAssignments = rewriteAndReOrderAssignments(update.assignments, sourceDf)
        rewriteCondition -> formatAssignments
      }).toMap
    // Serialize the Map[UpdateCondition, UpdateAssignments] to base64 string
    val serializedUpdateConditionAndExpressions = Base64.getEncoder
      .encodeToString(SerDeUtils.toBytes(updateConditionToAssignments))

    if (deleteAction.isDefined) {
      val deleteCondition = deleteAction.get.condition
        .map(replaceAttributeInExpression(_, sourceDf))
        .getOrElse(Literal.create(true, BooleanType))
      // Serialize the Map[DeleteCondition, empty] to base64 string
      val serializedDeleteCondition = Base64.getEncoder
        .encodeToString(SerDeUtils.toBytes(Map(deleteCondition -> Seq.empty[Assignment])))
      writeParams += (PAYLOAD_DELETE_CONDITION -> serializedDeleteCondition)
    }

    writeParams += (PAYLOAD_UPDATE_CONDITION_AND_ASSIGNMENTS ->
      serializedUpdateConditionAndExpressions)
    // Serialize the Map[InsertCondition, InsertAssignments] to base64 string
    writeParams += (PAYLOAD_INSERT_CONDITION_AND_ASSIGNMENTS ->
      serializedInsertConditionAndExpressions(insertActions, sourceDf))

    HoodieSparkSqlWriter.write(sparkSession.sqlContext, SaveMode.Append, writeParams, sourceDf)
  }

  /**
    * If there are not matched actions, we only execute the insert operation.
    * @param sourceDf
    * @param parameters
    */
  private def executeInsertOnly(sourceDf: DataFrame, parameters: Map[String, String]): Unit = {
    val insertActions = mergeInto.notMatchedActions.map(_.asInstanceOf[InsertAction])
    checkInsertAssignments(insertActions)

    var writeParams = parameters +
      (OPERATION_OPT_KEY -> INSERT_OPERATION_OPT_VAL) +
      (HoodieWriteConfig.WRITE_SCHEMA -> getWriteSchema.toString)

    writeParams += (PAYLOAD_INSERT_CONDITION_AND_ASSIGNMENTS ->
      serializedInsertConditionAndExpressions(insertActions, sourceDf))
    HoodieSparkSqlWriter.write(sparkSession.sqlContext, SaveMode.Append, writeParams, sourceDf)
  }

  private def checkUpdateAssignments(updateActions: Seq[UpdateAction]): Unit = {
    updateActions.foreach(update =>
      assert(update.assignments.length == targetTableSchemaWithoutMetaFields.length,
        s"The number of update assignments[${update.assignments.length}] must equal to the " +
          s"targetTable field size[${targetTableSchemaWithoutMetaFields.length}]"))
  }

  private def checkInsertAssignments(insertActions: Seq[InsertAction]): Unit = {
    insertActions.foreach(insert =>
      assert(insert.assignments.length == targetTableSchemaWithoutMetaFields.length,
        s"The number of insert assignments[${insert.assignments.length}] must equal to the " +
          s"targetTable field size[${targetTableSchemaWithoutMetaFields.length}]"))
  }

  private def getWriteSchema: Schema = {
    val (structName, nameSpace) = AvroConversionUtils
      .getAvroRecordNameAndNamespace(targetTableIdentify.identifier)
    AvroConversionUtils.convertStructTypeToAvroSchema(
      new StructType(targetTableSchemaWithoutMetaFields), structName, nameSpace)
  }

  /**
    * Serialize the Map[InsertCondition, InsertAssignments] to base64 string.
    * @param insertActions
    * @param sourceDf
    * @return
    */
  private def serializedInsertConditionAndExpressions(
          insertActions: Seq[InsertAction], sourceDf: DataFrame): String = {
    val insertConditionAndAssignments =
      insertActions.map(insert => {
        val rewriteCondition = insert.condition.map(replaceAttributeInExpression(_, sourceDf))
          .getOrElse(Literal.create(true, BooleanType))
        val formatAssignments = rewriteAndReOrderAssignments(insert.assignments, sourceDf)
        // do the check for the insert assignments
        checkInsertExpression(formatAssignments, sourceDf)

        rewriteCondition -> formatAssignments
      }).toMap
    Base64.getEncoder.encodeToString(
      SerDeUtils.toBytes(insertConditionAndAssignments))
  }

  /**
    * Rewrite and ReOrder the assignments.
    * The Rewrite is to replace the AttributeReference to BoundReference.
    * The ReOrder is to make the assignments's order same with the target table.
    * @param assignments
    * @param sourceDf
    * @return
    */
  private def rewriteAndReOrderAssignments(assignments: Seq[Expression],
          sourceDf: DataFrame): Seq[Expression] = {
    val name2Assignment = assignments.map {
      case Assignment(attr: AttributeReference, value) => {
        val targetColumnName = if (attr.qualifier.nonEmpty) {
          s"${attr.qualifier.mkString(".")}.${attr.name}"
        } else {
          attr.name
        }
        val rewriteValue = replaceAttributeInExpression(value, sourceDf)
        targetColumnName -> Alias(rewriteValue, attr.name)()
      }
      case assignment => throw new IllegalArgumentException(s"Illegal Assignment: ${assignment.sql}")
    }.toMap
   // reorder the assignments by the target table field
   targetTableSchemaWithoutMetaFields
      .map(field => {
        val assignment = name2Assignment.getOrElse(s"$targetAlias.${field.name}",
          name2Assignment.getOrElse(field.name, null))
        if (assignment != null) {
          castIfNeeded(assignment, field.dataType, sparkSession.sqlContext.conf)
        } else {
          throw new IllegalArgumentException(s"cannot find field ${field.name}" +
            s" in the assignments: ${assignments.mkString(",")}")
        }
      })
  }

  /**
    * Replace the AttributeReference to BoundReference. This is for the convenience of CodeGen
    * in ExpressionCodeGen which use the field index to generate the code. So we must replace
    * the AttributeReference to BoundReference here.
    * @param exp
    * @param sourceDf
    * @return
    */
  private def replaceAttributeInExpression(exp: Expression, sourceDf: DataFrame): Expression = {
    val sourceJoinTargetFields = sourceDf.schema.fields.map(f => s"$sourceAlias.${f.name}") ++
      targetTableSchemaWithoutMetaFields.map(f => s"$targetAlias.${f.name}")

    exp transform {
      case attr: AttributeReference =>
          val index = sourceJoinTargetFields.indexOf(attr.qualifiedName)
          if (index == -1) {
            throw new IllegalArgumentException(s"cannot find ${attr.qualifiedName} in source or " +
              s"target at the merge into statement")
          }
          BoundReference(index, attr.dataType, attr.nullable)
      case other => other
    }
  }

  private def checkInsertExpression(expressions: Seq[Expression], inputDf: DataFrame): Unit = {
    expressions.foreach(exp => {
      val references = exp.collect {
        case reference: BoundReference => reference
      }
      references.foreach(ref => {
        if (ref.ordinal >= inputDf.schema.fields.length) {
          throw new IllegalArgumentException(s"Insert clause cannot contain target table field")
        }
      })
    })
  }

  /**
    * Create the config for hoodie writer.
    * @param mergeInto
    * @return
    */
  private def buildMergeIntoConfig(mergeInto: MergeIntoTable): Map[String, String] = {

    val targetTableDb = targetTableIdentify.database.getOrElse("default")
    val targetTableName = targetTableIdentify.identifier
    val path = getTableLocation(targetTable, sparkSession)
      .getOrElse(s"missing location for $targetTableIdentify")

    val mergeEqualConditionKeys = getMergeEqulConditionKeys(mergeInto, targetTable)
    val options = targetTable.storage.properties
    val definedPk = HoodieOptionConfig.getPrimaryColumns(options)
    // TODO Currently the mergeEqualConditionKeys must be the same the primary key.
    if (mergeEqualConditionKeys.toSet != definedPk.toSet) {
      throw new IllegalArgumentException(s"Merge Key[${mergeEqualConditionKeys.mkString(",")}] is not" +
        s" Equal to the defined primary key[${definedPk.mkString(",")}] in table $targetTableName")
    }
    HoodieWriterUtils.parametersWithWriteDefaults(
      withSparkConf(sparkSession, options) {
        Map( // Since we have append the "Star" to the path when create table
             // (see CreateHoodieTableCommand), we must remove it before write.
          "path" -> removeStarFromPath(path),
          RECORDKEY_FIELD_OPT_KEY -> definedPk.mkString(","),
          KEYGENERATOR_CLASS_OPT_KEY -> classOf[ComplexKeyGenerator].getCanonicalName,
          PRECOMBINE_FIELD_OPT_KEY -> definedPk.head, // set a default preCombine field
          TABLE_NAME -> targetTableName,
          PARTITIONPATH_FIELD_OPT_KEY -> targetTable.partitionColumnNames.mkString(","),
          PAYLOAD_CLASS_OPT_KEY -> classOf[ExpressionPayload].getCanonicalName,
          META_SYNC_ENABLED_OPT_KEY -> "true",
          HIVE_USE_JDBC_OPT_KEY -> "false",
          HIVE_DATABASE_OPT_KEY -> targetTableDb,
          HIVE_TABLE_OPT_KEY -> targetTableName,
          HIVE_PARTITION_FIELDS_OPT_KEY -> targetTable.partitionColumnNames.mkString(","),
          HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> classOf[MultiPartKeysValueExtractor].getCanonicalName,
          URL_ENCODE_PARTITIONING_OPT_KEY -> "true", // enable the url decode for sql.
          INSERT_PARALLELISM -> "64", // set the default parallelism to 64
          UPSERT_PARALLELISM -> "64",
          DELETE_PARALLELISM -> "64"
        )
      })
  }

  /**
    *
    * Get the equal condition keys in the merge condition.
    * e.g. merge on t.id = s.id AND t.name = s.name, we return ("id", "name").
    * TODO Currently Non-equivalent conditions are not supported.
    * @param mergeInto
    * @return
    */
  private def getMergeEqulConditionKeys(
    mergeInto: MergeIntoTable, targetTable: CatalogTable): Seq[String] = {
    val conditions = splitByAnd(mergeInto.mergeCondition)
    val allEqs = conditions.forall(p => p.isInstanceOf[EqualTo])
    if (!allEqs) {
      throw new IllegalArgumentException(s"None equal condition is not support for Merge Into")
    }
    val keys = conditions.map(_.asInstanceOf[EqualTo]).map { eq =>
      var key: String = eq.left match {
        case attr: AttributeReference =>
          if (attr.qualifier.mkString(".") == targetAlias) {
            attr.name
          } else null
        case Cast(attr: AttributeReference, _, _) =>
          if (attr.qualifier.mkString(".") == targetAlias) {
            attr.name
          } else null
        case left => throw new IllegalArgumentException(
          s"left of equalTo in condition must be a field, current is ${left.sql}")
      }
      if (key == null) {
        key = eq.right match {
          case attr: AttributeReference =>
            if (attr.qualifier.mkString(".") == targetAlias) {
              attr.name
            } else {
              throw new IllegalArgumentException(s"Can't identify : ${eq.sql} in merge " +
                s"condition, it must have a alias")
            }
          case Cast(attr: AttributeReference, _, _) =>
            if (attr.qualifier.mkString(".") == targetAlias) {
              attr.name
            } else {
              throw new IllegalArgumentException(s"Can't identify: ${eq.sql}, it must have a alias")
            }
          case right => throw new IllegalArgumentException(
            s"right of equalTo in condition must be a field, current is ${right.sql}")
        }
      }
      key
    }
    keys
  }
}
