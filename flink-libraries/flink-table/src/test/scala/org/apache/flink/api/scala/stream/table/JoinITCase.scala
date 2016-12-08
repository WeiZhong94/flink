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
package org.apache.flink.api.scala.stream.table

import org.apache.flink.api.scala.stream.utils.{StreamTestData}
import org.apache.flink.api.table.{Row, TableEnvironment}
import org.apache.flink.api.table.utils.TableTestBase

import scala.collection.mutable
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.stream.utils.StreamITCase
import org.apache.flink.api.scala.table._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.junit.Test

class JoinITCase extends TableTestBase {
  @Test
  def testStreamJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e).select('a, 'b, 'c, 'd, 'e, 'f, 'g, 'h)
    val results = joinT.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    println("================result[START]==================")
    for (data <- StreamITCase.testResults.sorted) {
      println(data)
    }
    println("================result[END]==================")
  }
}
