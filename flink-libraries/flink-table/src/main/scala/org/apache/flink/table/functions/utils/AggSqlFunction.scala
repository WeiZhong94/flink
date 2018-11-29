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

package org.apache.flink.table.functions.utils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.AggregateFunction

/**
  * Calcite wrapper for user-defined aggregate functions.
  *
  * @param name function name (used by SQL parser)
  * @param displayName name to be displayed in operator name
  * @param aggregateFunction aggregate function to be called
  * @param returnType the type information of returned value
  * @param accType the type information of the accumulator
  * @param typeFactory type factory for converting Flink's between Calcite's types
  */
class AggSqlFunction (
    name: String,
    displayName: String,
    aggregateFunction: AggregateFunction[_, _],
    override val returnType: TypeInformation[_],
    override val accType: TypeInformation[_],
    typeFactory: FlinkTypeFactory,
    requiresOver: Boolean)
  extends AccumulateSqlFunction(
    name,
    displayName,
    aggregateFunction,
    returnType,
    accType,
    typeFactory,
    requiresOver
  ) {

  override def getFunction: AggregateFunction[_, _] = aggregateFunction
}

object AggSqlFunction {
  def apply(
      name: String,
      displayName: String,
      aggregateFunction: AggregateFunction[_, _],
      returnType: TypeInformation[_],
      accType: TypeInformation[_],
      typeFactory: FlinkTypeFactory,
      requiresOver: Boolean): AggSqlFunction = {

    new AggSqlFunction(
      name,
      displayName,
      aggregateFunction,
      returnType,
      accType,
      typeFactory,
      requiresOver)
  }
}
