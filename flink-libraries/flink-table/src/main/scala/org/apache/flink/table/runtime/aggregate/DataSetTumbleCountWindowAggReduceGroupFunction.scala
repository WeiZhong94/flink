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
package org.apache.flink.table.runtime.aggregate

import java.lang.Iterable

import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.runtime.aggregate.DataSetTumbleCountWindowAggReduceGroupFunction.WrappedCollector
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * It wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]].
  * It is only used for tumbling count-window on batch.
  *
  * @param genAggregations  Code-generated [[GeneratedAggregations]]
  * @param windowSize       Tumble count window size
  */
class DataSetTumbleCountWindowAggReduceGroupFunction(
    private val genAggregations: GeneratedAggregationsFunction,
    private val windowSize: Long,
    isTableAgg: Boolean = false)
  extends RichGroupReduceFunction[Row, Row]
    with Compiler[GeneratedAggregations]
    with Logging {

  private var output: Row = _
  private var accumulators: Row = _

  private var function: GeneratedAggregations = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling AggregateHelper: $genAggregations.name \n\n " +
                s"Code:\n$genAggregations.code")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()

    output = function.createOutputRow()
    accumulators = function.createAccumulators()
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    var count: Long = 0
    val iterator = records.iterator()

    while (iterator.hasNext) {

      if (count == 0) {
        function.resetAccumulator(accumulators)
      }

      val record = iterator.next()
      count += 1

      accumulators = function.mergeAccumulatorsPair(accumulators, record)

      if (windowSize == count) {
        if (!isTableAgg) {
          // set group keys value to final output.
          function.setForwardedFields(record, output)

          function.setAggregationResults(accumulators, output)
          // emit the output
          out.collect(output)
        } else {
          val wrappedCollector = new WrappedCollector
          wrappedCollector.collector = out
          wrappedCollector.forwardRecord = record
          wrappedCollector.function = function
          function.setAggregationResults(accumulators, wrappedCollector)
        }
        count = 0
      }
    }
  }
}

object DataSetTumbleCountWindowAggReduceGroupFunction {
  class WrappedCollector extends Collector[Row] {
    var collector: Collector[Row] = _
    var forwardRecord: Row = _
    var function: GeneratedAggregations = _

    /**
      * Emits a record.
      *
      * @param record The record to collect.
      */
    override def collect(output: Row): Unit = {
      function.setForwardedFields(forwardRecord, output)
      collector.collect(output)
    }

    /**
      * Closes the collector. If any data was buffered, that data will be flushed.
      */
    override def close(): Unit = {}
  }
}
