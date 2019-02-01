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

package org.apache.flink.table.plan.expressions

import org.apache.flink.table.api._
import org.apache.flink.table.api.scala.{CurrentRange, CurrentRow, UnboundedRange, UnboundedRow}
import org.apache.flink.table.expressions._

object ScalaExpressionParser {
  def parse(expr: Expression): PlannerExpression = {
    if (expr == null) {
      return null
    }
    expr match {
      case DistinctAgg(child) =>
        PlannerDistinctAgg(parse(child))

      case Call(func, args) =>
        //PlannerCall(functionName, args.map(parse))
        func match {
          case e: ScalarFunctionDefinition =>
            PlannerScalarFunctionCall(e.func, args.map(parse))

          case e: AggFunctionDefinition =>
            PlannerAggFunctionCall(e.func, e.resultTypeInfo, e.accTypeInfo, args.map(parse))

          case e: BuildInFunctionDefinition =>
            if (e.reuseJavaFunctionCatalog) {
              PlannerCall(e.name, args.map(parse))
            } else {
              e match {
                case FunctionDefinitions.CAST =>
                  assert(args.size == 2)
                  PlannerCast(parse(args.head), args.last.asInstanceOf[TypeLiteral].t)

                case FunctionDefinitions.FlATTENING =>
                  assert(args.size == 1)
                  PlannerFlattening(parse(args.head))

                case FunctionDefinitions.GET_COMPOSITE_FIELD =>
                  assert(args.size == 2)
                  PlannerGetCompositeField(parse(args.head), args.last.asInstanceOf[Literal].l)

                case FunctionDefinitions.IN =>
                  PlannerIn(parse(args.head), args.slice(1, args.size).map(parse))
              }
            }
        }

      case UnresolvedOverCall(agg, alias) =>
        PlannerUnresolvedOverCall(parse(agg), parse(alias))

      case UnresolvedFieldReference(name) =>
        PlannerUnresolvedFieldReference(name)

      case Alias(child, name, extraNames) =>
        PlannerAlias(parse(child), name, extraNames)

      case TableReference(name, table) =>
        PlannerTableReference(name, table)

      case RowtimeAttribute(expression) =>
        PlannerRowtimeAttribute(parse(expression))

      case ProctimeAttribute(expression) =>
        PlannerProctimeAttribute(parse(expression))

      case StreamRecordTimestamp() =>
        PlannerStreamRecordTimestamp()

      case Literal(l, None) =>
        PlannerLiteral(l)

      case Literal(l, Some(t)) =>
        PlannerLiteral(l, t)

      case Null(resultType) =>
        PlannerNull(resultType)

      case CurrentRow() =>
        PlannerCurrentRow()

      case CurrentRange() =>
        PlannerCurrentRange()

      case UnboundedRow() =>
        PlannerUnboundedRow()

      case UnboundedRange() =>
        PlannerUnboundedRange()

      case SymbolExpression(symbol) =>
        val tableSymbol = symbol match {
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
            throw new TableException("unsupported TableSymbolValue: " + symbol)
        }
        PlannerSymbolExpression(tableSymbol)

      case _ =>
        throw new TableException("unsupported Expression: " + expr.getClass.getSimpleName)
    }
  }
}
