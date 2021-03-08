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

package org.apache.spark.sql.hudi.analysis

import scala.collection.JavaConverters._
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, InsertIntoTable, Join, LogicalPlan, MergeIntoTable, Project, UpdateAction}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.CreateDataSourceTableCommand
import org.apache.spark.sql.execution.datasources.{CreateTable, LogicalRelation}
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.hudi.command.{CreateHoodieTableAsSelectCommand, CreateHoodieTableCommand, InsertIntoHoodieTableCommand, MergeIntoHoodieTableCommand}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StringType

object HoodieAnalysis {
  def customResolutionRules(): Seq[SparkSession => Rule[LogicalPlan]] =
    Seq(
      session => HoodieResolveReferences(session),
      session => HoodieAnalysis(session)
    )

  def customPostHocResolutionRules(): Seq[SparkSession => Rule[LogicalPlan]] =
    Seq(
      session => HoodiePostAnalysisRule(session)
    )
}

/**
  * Rule for convert the logical plan to command.
  * @param sparkSession
  */
case class HoodieAnalysis(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      // Convert to MergeIntoHoodieTableCommand
      case m @ MergeIntoTable(target, _, _, _, _)
        if m.resolved && isHoodieTable(target, sparkSession) =>
          MergeIntoHoodieTableCommand(m)

      // Convert to InsertIntoHoodieTableCommand
      case _ @ InsertIntoTable(
        l @ LogicalRelation(_: BaseRelation, _, table, _), parts, query, overwrite, _)
        if table.isDefined && isHoodieTable(table.get) =>
        new InsertIntoHoodieTableCommand(l, query, parts, overwrite)

      // Convert to CreateHoodieTableAsSelectCommand
      case CreateTable(table, mode, Some(query))
        if query.resolved && isHoodieTable(table) =>
        CreateHoodieTableAsSelectCommand(table, mode, query)
      case _=> plan
    }
  }
}

/**
  * Rule for resolve hoodie's extended syntax or rewrite some logical plan.
  * @param sparkSession
  */
case class HoodieResolveReferences(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private lazy val analyzer = sparkSession.sessionState.analyzer

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      // Resolve merge into
      case MergeIntoTable(target, source, mergeCondition, matchedActions, notMatchedActions)
        if isHoodieTable(target, sparkSession) && target.resolved && source.resolved =>

        def resolveConditionAssignments(condition: Option[Expression],
            assignments: Seq[Assignment]): (Option[Expression], Seq[Assignment]) = {
          val resolvedCondition = condition.map(resolveExpressionFrom(source)(_))
          val resolvedAssignments = if (assignments.isEmpty) {
            // assignments is empty means insert * or update set *
            // we fill assign all the source fields to the target fields
            target.output
              .filter(attr => !HoodieRecord.HOODIE_META_COLUMNS.contains(attr.name))
              .map { targetAttr =>
                // resolve the field in the source using the target field name.
                // In the case of insert * or update set *, we use the target field
                // name to match to source field.
                val sourceAttr =
                  resolveExpressionFrom(source)(UnresolvedAttribute(targetAttr.name))
               Assignment(targetAttr, sourceAttr)
              }
          } else {
            assignments.map(assignment => {
              val resolvedKey = resolveExpressionFrom(target)(assignment.key)
              val resolvedValue = resolveExpressionFrom(source, Some(target))(assignment.value)
              Assignment(resolvedKey, resolvedValue)
            })
          }
          (resolvedCondition, resolvedAssignments)
        }

        // Resolve the merge condition
        val resolvedMergeCondition = resolveExpressionFrom(source, Some(target))(mergeCondition)

        // Resolve the matchedActions
        val resolvedMatchedActions = matchedActions.map {
          case UpdateAction(condition, assignments) =>
            val (resolvedCondition, resolvedAssignments) =
              resolveConditionAssignments(condition, assignments)
            UpdateAction(resolvedCondition, resolvedAssignments)
          case DeleteAction(condition) =>
            val resolvedCondition = condition.map(resolveExpressionFrom(source)(_))
            DeleteAction(resolvedCondition)
        }
        // Resolve the notMatchedActions
        val resolvedNotMatchedActions = notMatchedActions.map {
          case InsertAction(condition, assignments) =>
            val (resolvedCondition, resolvedAssignments) =
              resolveConditionAssignments(condition, assignments)
            InsertAction(resolvedCondition, resolvedAssignments)
        }
        // Return the resolved MergeIntoTable
        MergeIntoTable(target, source, resolvedMergeCondition,
          resolvedMatchedActions, resolvedNotMatchedActions)

      // Append the meta field to the insert query to walk through the validate for the
      // number of insert fields with the number of the target table fields.
      case InsertIntoTable(table : LogicalPlan, partition, query: Project,
        overwrite, ifPartitionNotExists)
        if isHoodieTable(table, sparkSession) &&
          !containUnResolvedStar(query) && !checkAlreadyAppendMetaField(query) =>
        val project = removeMetaField(query)

        val withMetaFieldProjects =
          HoodieRecord.HOODIE_META_COLUMNS.asScala.map(
            Alias(Literal.create(null, StringType), _)()).toArray[NamedExpression] ++
            project.projectList
        // Append the meta fileds to the insert query.
        val newQuery = Project(withMetaFieldProjects, project.child)
        InsertIntoTable(table, partition, newQuery, overwrite, ifPartitionNotExists)
      case _ => plan
    }
  }

  private def containUnResolvedStar(project: Project): Boolean = {
    project.projectList.exists(_.isInstanceOf[UnresolvedStar])
  }

  /**
    * Check if the the project has already append the meta fields to avoid duplicate append.
    * @param project
    * @return
    */
  private def checkAlreadyAppendMetaField(project: Project): Boolean = {
    project.projectList.filter(isMetaField)
      .map {
        case Alias(_, name) => name.toLowerCase
        case AttributeReference(name, _, _, _) => name.toLowerCase
        case other => throw new IllegalArgumentException(s"$other should not be a hoodie meta field")
      }.toSet == HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet
  }

  private def removeMetaField(project: Project): Project = {
    val filteredProjects = project.projectList.filterNot(isMetaField)
    Project(filteredProjects, project.child)
  }

  private def isMetaField(exp: Expression): Boolean = {
    val metaFields = HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet
    exp match {
      case Alias(_, name) if metaFields.contains(name.toLowerCase) => true
      case AttributeReference(name, _, _, _) if metaFields.contains(name.toLowerCase) => true
      case _=> false
    }
  }

  /**
    * Resolve the expression.
    * 1、 Fake a a project for the expression based on the source plan
    * 2、 Resolve the fake project
    * 3、 Get the resolved expression from the faked project
    * @param left The left source plan for the expression.
    * @param right The right source plan for the expression.
    * @param expression The expression to resolved.
    * @return The resolved expression.
    */
  private def resolveExpressionFrom(left: LogicalPlan, right: Option[LogicalPlan] = None)
                        (expression: Expression): Expression = {
    // Fake a project for the expression based on the source plan.
    val fakeProject = if (right.isDefined) {
      Project(Seq(Alias(expression, "_c0")()),
        Join(left, right.get, Inner, None))
    } else {
      Project(Seq(Alias(expression, "_c0")()),
        left)
    }
    // Resolve the fake project
    val resolvedProject =
      analyzer.ResolveReferences.apply(fakeProject).asInstanceOf[Project]
    // Fetch the resolved expression from the fake project.
    resolvedProject.projectList.head.asInstanceOf[Alias].child
  }
}

/**
  * Rule for rewrite some spark commands to hudi's implementation.
  * @param sparkSession
  */
case class HoodiePostAnalysisRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      // Rewrite the CreateDataSourceTableCommand to CreateHoodieTableCommand
      case CreateDataSourceTableCommand(table, ignoreIfExists)
        if isHoodieTable(table) =>
        CreateHoodieTableCommand(table, ignoreIfExists)
      case _ => plan
    }
  }
}
