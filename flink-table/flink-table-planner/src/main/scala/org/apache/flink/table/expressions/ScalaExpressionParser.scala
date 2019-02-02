/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.expressions

import org.apache.flink.table.api._
import org.apache.flink.table.plan.expressions._

import _root_.scala.collection.JavaConverters._

object ScalaExpressionParser {
  def parse(expr: Expression): PlannerExpression = {
    if (expr == null) {
      return null
    }
    expr match {
      case e: DistinctAgg =>
        PlannerDistinctAgg(parse(e.getChild))

      case e: Call =>
        val func = e.getFunc
        val args = e.getChildren.asScala
        func match {
          case e: ScalarFunctionDefinition =>
            PlannerScalarFunctionCall(e.getFunc, args.map(parse))

          case e: AggFunctionDefinition =>
            PlannerAggFunctionCall(
              e.getFunc, e.getResultTypeInfo, e.getAccTypeInfo, args.map(parse))

          case e: BuildInFunctionDefinition =>
            if (e.isReuseJavaFunctionCatalog) {
              PlannerCall(e.getName, args.map(parse))
            } else {
              e match {
                case FunctionDefinitions.CAST =>
                  assert(args.size == 2)
                  PlannerCast(parse(args.head), args.last.asInstanceOf[TypeLiteral].getType)

                case FunctionDefinitions.FLATTENING =>
                  assert(args.size == 1)
                  PlannerFlattening(parse(args.head))

                case FunctionDefinitions.GET_COMPOSITE_FIELD =>
                  assert(args.size == 2)
                  PlannerGetCompositeField(parse(args.head),
                    args.last.asInstanceOf[Literal].getValue)

                case FunctionDefinitions.IN =>
                  PlannerIn(parse(args.head), args.slice(1, args.size).map(parse))

                case _ =>
                  throw new TableException("unsupported FunctionDefinition: " + e)
              }
            }
        }

      case e: UnresolvedOverCall =>
        PlannerUnresolvedOverCall(parse(e.getLeft), parse(e.getRight))

      case e: UnresolvedFieldReference =>
        PlannerUnresolvedFieldReference(e.getName)

      case e: Alias =>
        PlannerAlias(parse(e.getChild), e.getName, e.getExtraNames.asScala)

      case e: TableReference =>
        PlannerTableReference(e.getName, e.getTable)

      case e: RowtimeAttribute =>
        PlannerRowtimeAttribute(parse(e.getChild))

      case e: ProctimeAttribute =>
        PlannerProctimeAttribute(parse(e.getChild))

      case e: Literal =>
        if (!e.getType.isPresent) {
          PlannerLiteral(e.getValue)
        } else {
          PlannerLiteral(e.getValue, e.getType.get())
        }

      case e: Null =>
        PlannerNull(e.getType)

      case e: CurrentRow =>
        PlannerCurrentRow()

      case e: CurrentRange =>
        PlannerCurrentRange()

      case e: UnboundedRow =>
        PlannerUnboundedRow()

      case e: UnboundedRange =>
        PlannerUnboundedRange()

      case e: SymbolExpression =>
        val tableSymbol = e.getSymbol match {
          case TimeIntervalUnit.YEAR => PlannerTimeIntervalUnit.YEAR
          case TimeIntervalUnit.YEAR_TO_MONTH => PlannerTimeIntervalUnit.YEAR_TO_MONTH
          case TimeIntervalUnit.QUARTER => PlannerTimeIntervalUnit.QUARTER
          case TimeIntervalUnit.MONTH => PlannerTimeIntervalUnit.MONTH
          case TimeIntervalUnit.WEEK => PlannerTimeIntervalUnit.WEEK
          case TimeIntervalUnit.DAY => PlannerTimeIntervalUnit.DAY
          case TimeIntervalUnit.DAY_TO_HOUR => PlannerTimeIntervalUnit.DAY_TO_HOUR
          case TimeIntervalUnit.DAY_TO_MINUTE => PlannerTimeIntervalUnit.DAY_TO_MINUTE
          case TimeIntervalUnit.DAY_TO_SECOND => PlannerTimeIntervalUnit.DAY_TO_SECOND
          case TimeIntervalUnit.HOUR => PlannerTimeIntervalUnit.HOUR
          case TimeIntervalUnit.HOUR_TO_MINUTE => PlannerTimeIntervalUnit.HOUR_TO_MINUTE
          case TimeIntervalUnit.HOUR_TO_SECOND => PlannerTimeIntervalUnit.HOUR_TO_SECOND
          case TimeIntervalUnit.MINUTE => PlannerTimeIntervalUnit.MINUTE
          case TimeIntervalUnit.MINUTE_TO_SECOND => PlannerTimeIntervalUnit.MINUTE_TO_SECOND
          case TimeIntervalUnit.SECOND => PlannerTimeIntervalUnit.SECOND

          case TimePointUnit.YEAR => PlannerTimePointUnit.YEAR
          case TimePointUnit.MONTH => PlannerTimePointUnit.MONTH
          case TimePointUnit.DAY => PlannerTimePointUnit.DAY
          case TimePointUnit.HOUR => PlannerTimePointUnit.HOUR
          case TimePointUnit.MINUTE => PlannerTimePointUnit.MINUTE
          case TimePointUnit.SECOND => PlannerTimePointUnit.SECOND
          case TimePointUnit.QUARTER => PlannerTimePointUnit.QUARTER
          case TimePointUnit.WEEK => PlannerTimePointUnit.WEEK
          case TimePointUnit.MILLISECOND => PlannerTimePointUnit.MILLISECOND
          case TimePointUnit.MICROSECOND => PlannerTimePointUnit.MICROSECOND

          case TrimMode.BOTH => PlannerTrimMode.BOTH
          case TrimMode.LEADING => PlannerTrimMode.LEADING
          case TrimMode.TRAILING => PlannerTrimMode.TRAILING

          case _ =>
            throw new TableException("unsupported TableSymbolValue: " + e.getSymbol)
        }
        PlannerSymbolExpression(tableSymbol)

      case _ =>
        throw new TableException("unsupported Expression: " + expr.getClass.getSimpleName)
    }
  }
}
