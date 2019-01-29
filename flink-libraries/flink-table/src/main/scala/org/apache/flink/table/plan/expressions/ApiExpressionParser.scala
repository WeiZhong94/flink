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
import org.apache.flink.table.apiexpressions._

object ApiExpressionParser {
  def parse(expr: ApiExpression): PlannerExpression = {
    if (expr == null) {
      return null
    }
    expr match {
      case ApiDistinctAgg(child) =>
        PlannerDistinctAgg(parse(child))

      case ApiAggFunctionCall(function, resultTypeInfo, accTypeInfo, args) =>
        PlannerAggFunctionCall(function, resultTypeInfo, accTypeInfo, args.map(parse))

      case ApiCall(functionName, args) =>
        PlannerCall(functionName, args.map(parse))

      case ApiUnresolvedOverCall(agg, alias) =>
        PlannerUnresolvedOverCall(parse(agg), parse(alias))

      case ApiScalarFunctionCall(scalarFunction, parameters) =>
        PlannerScalarFunctionCall(scalarFunction, parameters.map(parse))

      case ApiTableFunctionCall(functionName, tableFunction, parameters, resultType) =>
        PlannerTableFunctionCall(functionName, tableFunction, parameters.map(parse), resultType)

      case ApiCast(child, resultType) =>
        PlannerCast(parse(child), resultType)

      case ApiFlattening(child) =>
        PlannerFlattening(parse(child))

      case ApiGetCompositeField(child, key) =>
        PlannerGetCompositeField(parse(child), key)

      case ApiUnresolvedFieldReference(name) =>
        UnresolvedFieldReference(name)

      case ApiAlias(child, name, extraNames) =>
        PlannerAlias(parse(child), name, extraNames)

      case ApiTableReference(name, table) =>
        PlannerTableReference(name, table)

      case ApiRowtimeAttribute(expression) =>
        PlannerRowtimeAttribute(parse(expression))

      case ApiProctimeAttribute(expression) =>
        PlannerProctimeAttribute(parse(expression))

      case ApiStreamRecordTimestamp() =>
        PlannerStreamRecordTimestamp()

      case ApiLiteral(l, None) =>
        PlannerLiteral(l)

      case ApiLiteral(l, Some(t)) =>
        PlannerLiteral(l, t)

      case ApiNull(resultType) =>
        PlannerNull(resultType)

      case ApiIn(expression, elements) =>
        PlannerIn(parse(expression), elements.map(parse))

      case ApiCurrentRow() =>
        PlannerCurrentRow()

      case ApiCurrentRange() =>
        PlannerCurrentRange()

      case ApiUnboundedRow() =>
        PlannerUnboundedRow()

      case ApiUnboundedRange() =>
        PlannerUnboundedRange()

      case ApiSymbolExpression(symbol) =>
        val tableSymbol = symbol match {
          case ApiTimeIntervalUnit.YEAR => PlannerTimeIntervalUnit.YEAR
          case ApiTimeIntervalUnit.YEAR_TO_MONTH => PlannerTimeIntervalUnit.YEAR_TO_MONTH
          case ApiTimeIntervalUnit.QUARTER => PlannerTimeIntervalUnit.QUARTER
          case ApiTimeIntervalUnit.MONTH => PlannerTimeIntervalUnit.MONTH
          case ApiTimeIntervalUnit.WEEK => PlannerTimeIntervalUnit.WEEK
          case ApiTimeIntervalUnit.DAY => PlannerTimeIntervalUnit.DAY
          case ApiTimeIntervalUnit.DAY_TO_HOUR => PlannerTimeIntervalUnit.DAY_TO_HOUR
          case ApiTimeIntervalUnit.DAY_TO_MINUTE => PlannerTimeIntervalUnit.DAY_TO_MINUTE
          case ApiTimeIntervalUnit.DAY_TO_SECOND => PlannerTimeIntervalUnit.DAY_TO_SECOND
          case ApiTimeIntervalUnit.HOUR => PlannerTimeIntervalUnit.HOUR
          case ApiTimeIntervalUnit.HOUR_TO_MINUTE => PlannerTimeIntervalUnit.HOUR_TO_MINUTE
          case ApiTimeIntervalUnit.HOUR_TO_SECOND => PlannerTimeIntervalUnit.HOUR_TO_SECOND
          case ApiTimeIntervalUnit.MINUTE => PlannerTimeIntervalUnit.MINUTE
          case ApiTimeIntervalUnit.MINUTE_TO_SECOND => PlannerTimeIntervalUnit.MINUTE_TO_SECOND
          case ApiTimeIntervalUnit.SECOND => PlannerTimeIntervalUnit.SECOND

          case ApiTimePointUnit.YEAR => PlannerTimePointUnit.YEAR
          case ApiTimePointUnit.MONTH => PlannerTimePointUnit.MONTH
          case ApiTimePointUnit.DAY => PlannerTimePointUnit.DAY
          case ApiTimePointUnit.HOUR => PlannerTimePointUnit.HOUR
          case ApiTimePointUnit.MINUTE => PlannerTimePointUnit.MINUTE
          case ApiTimePointUnit.SECOND => PlannerTimePointUnit.SECOND
          case ApiTimePointUnit.QUARTER => PlannerTimePointUnit.QUARTER
          case ApiTimePointUnit.WEEK => PlannerTimePointUnit.WEEK
          case ApiTimePointUnit.MILLISECOND => PlannerTimePointUnit.MILLISECOND
          case ApiTimePointUnit.MICROSECOND => PlannerTimePointUnit.MICROSECOND

          case ApiTrimMode.BOTH => PlannerTrimMode.BOTH
          case ApiTrimMode.LEADING => PlannerTrimMode.LEADING
          case ApiTrimMode.TRAILING => PlannerTrimMode.TRAILING

          case _ =>
            throw new TableException("unsupported TableSymbolValue: " + symbol)
        }
        PlannerSymbolExpression(tableSymbol)

      case _ =>
        throw new TableException("unsupported Expression: " + expr.getClass.getSimpleName)
    }
  }
}
