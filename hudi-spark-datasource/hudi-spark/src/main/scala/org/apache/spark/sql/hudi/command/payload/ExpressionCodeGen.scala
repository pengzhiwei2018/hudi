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

import java.util.UUID

import org.apache.avro.generic.IndexedRecord
import org.apache.hudi.sql.IExpressionEvaluator
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, LeafExpression, Literal, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.hudi.command.payload.ExpressionCodeGen.RECORD_NAME
import org.apache.spark.sql.types.{DataType, Decimal, StringType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.ParentClassLoader
import org.apache.spark.{TaskContext, TaskKilledException}
import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino.{ClassBodyEvaluator, InternalCompilerException}

/**
  * Do CodeGen for expression based on IndexedRecord.
  * The mainly difference with the spark's CodeGen for expression is that
  * the expression's input is a IndexedRecord but not a Row.
  *
  */
object ExpressionCodeGen extends Logging {

  val RECORD_NAME = "record"

  /**
    * CodeGen for expressions.
    * @param exprs The expression list to CodeGen.
    * @return An IExpressionEvaluator generate by CodeGen which take a IndexedRecord as input
    *         param and return a Array of results for each expression.
    */
  def doCodeGen(exprs: Seq[Expression]): IExpressionEvaluator = {
    val ctx = new CodegenContext()
    // Set the input_row to null as we do not use row as the input object but Record.
    ctx.INPUT_ROW = null

    val replacedExprs = exprs.map(replaceBoundReferenceAndLiteral)
    val resultVars = replacedExprs.map(_.genCode(ctx))
    val className = s"ExpressionPayloadEvaluator_${UUID.randomUUID().toString.replace("-", "_")}"
    val codeBody =
      s"""
         |private Object[] references;
         |
         |public $className(Object references) {
         |  this.references = (Object[])references;
         |}
         |
         |public Object[] eval(IndexedRecord $RECORD_NAME) {
         |    ${resultVars.map(_.code).mkString("\n")}
         |    Object[] results = new Object[${resultVars.length}];
         |    ${
                (for (i <- resultVars.indices) yield {
                          s"""
                             |if (${resultVars(i).isNull}) {
                             |  results[$i] = null;
                             |} else {
                             |  results[$i] = ${resultVars(i).value.code};
                             |}
                       """.stripMargin
                 }).mkString("\n")
              }
              return results;
         |  }
     """.stripMargin

    val evaluator = new ClassBodyEvaluator()
    val parentClassLoader = new ParentClassLoader(
      Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader))

    evaluator.setParentClassLoader(parentClassLoader)
    // Cannot be under package codegen, or fail with java.lang.InstantiationException
    evaluator.setClassName(s"org.apache.hudi.sql.payload.$className")
    evaluator.setDefaultImports(
      classOf[Platform].getName,
      classOf[InternalRow].getName,
      classOf[UnsafeRow].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[UnsafeArrayData].getName,
      classOf[MapData].getName,
      classOf[UnsafeMapData].getName,
      classOf[Expression].getName,
      classOf[TaskContext].getName,
      classOf[TaskKilledException].getName,
      classOf[InputMetrics].getName,
      classOf[IndexedRecord].getName
    )
    evaluator.setImplementedInterfaces(Array(classOf[IExpressionEvaluator]))
    try {
      evaluator.cook(codeBody)
    } catch {
      case e: InternalCompilerException =>
        val msg = s"failed to compile: $e"
        logError(msg, e)
        throw new InternalCompilerException(msg, e)
      case e: CompileException =>
        val msg = s"failed to compile: $e"
        logError(msg, e)
        throw new CompileException(msg, e.getLocation)
    }
    // Replace the UTF8String to Java String for the references.
    val referenceArray = ctx.references.toArray.map {
      case s: UTF8String => s.toString
      case o => o
    }.map(_.asInstanceOf[Object])
    evaluator.getClazz.getConstructor(classOf[Object])
      .newInstance(referenceArray)
      .asInstanceOf[IExpressionEvaluator]
  }

  /**
    * Replace the BoundReference and Literal to the Record implement which will override the
    * doGenCode method.
    * @param expression
    * @return
    */
  private def replaceBoundReferenceAndLiteral(expression: Expression): Expression = {
    expression transformDown  {
      case BoundReference(ordinal, dataType, nullable) =>
         RecordBoundReference(ordinal, dataType, nullable)
      case l: Literal =>
         RecordLiteral(l)
      case other =>
        other
    }
  }
}

/**
  * Since the string type returned by IndexRecord is java "string" not a "UTF8String" type,
  * we override the doGenCode for Literal to use "String" but not "UTF8String" as the CodeGen
  * type for string literal.
  */
case class RecordLiteral(literal: Literal) extends LeafExpression {

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    dataType match {
      case StringType =>
        val constRef = ctx.addReferenceObj("literal", literal.value, "String")
        ExprCode.forNonNullValue(JavaCode.global(constRef, dataType))
      case _ => literal.doGenCode(ctx, ev)
    }
  }

  override def nullable: Boolean = literal.nullable

  override def eval(input: InternalRow): Any = literal.eval(input)

  override def dataType: DataType = literal.dataType
}

case class RecordBoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends LeafExpression {

  /**
    * Do the CodeGen for RecordBoundReference.
    * 1) Use "String" as the java type for StringType but not "UTF8String"
    * 2) Use "IndexedRecord" as the input object but not a "Row"
    */
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = dataType match {
      case StringType => Inline("String") // use String as the java type
      case _=> JavaCode.javaType(dataType)
    }
    val boxType = dataType match {
      case StringType => Inline("String") // use String as the java type
      case _=> JavaCode.boxedType(dataType)
    }
    val value = s"($boxType)$RECORD_NAME.get($ordinal)"
    if (nullable) {
      ev.copy(code =
        code"""
              | boolean ${ev.isNull} = $RECORD_NAME.get($ordinal) == null;
              | $javaType ${ev.value} = ${ev.isNull} ?
              | ${CodeGenerator.defaultValue(dataType)} : ($value);
          """
      )
    } else {
      ev.copy(code = code"$javaType ${ev.value} = $value;", isNull = FalseLiteral)
    }
  }

  override def eval(input: InternalRow): Any = {
    throw new IllegalArgumentException(s"Should not call eval method for " +
      s"${getClass.getCanonicalName}")
  }
}

