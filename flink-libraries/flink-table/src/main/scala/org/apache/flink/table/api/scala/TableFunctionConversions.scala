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

package org.apache.flink.table.api.scala

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{Table, TableImpl}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.plan.expressions.ScalaExpressionParser
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.plan.logical.LogicalTableFunctionCall

/**
  * Holds methods to convert a [[TableFunction]] call in the Scala Table API into a [[Table]].
  *
  * @param tf The TableFunction to convert.
  */
class TableFunctionConversions[T](tf: TableFunction[T]) {

  /**
    * Creates a [[Table]] from a [[TableFunction]] in Scala Table API.
    *
    * @param args The arguments of the table function call.
    * @return A [[Table]] with which represents the [[LogicalTableFunctionCall]].
    */
  final def apply(args: Expression*)(implicit typeInfo: TypeInformation[T]): Table = {

    val resultType = if (tf.getResultType == null) typeInfo else tf.getResultType

    new TableImpl(
      tableEnv = null, // Table environment will be set later.
      LogicalTableFunctionCall(
        tf.getClass.getCanonicalName,
        tf,
        args.map(ScalaExpressionParser.parse).toList,
        resultType,
        Array.empty,
        child = null // Child will be set later.
      )
    )
  }
}
