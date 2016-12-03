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
package org.apache.flink.api.table.runtime.aggregate

import java.lang.Iterable

import org.apache.flink.api.common.functions.RichGroupCombineFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.table.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.{Collector, Preconditions}

import scala.collection.JavaConversions._

/**
  * It wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupCombineOperator]].
  *
  * @param aggregates       The aggregate functions.
  */
class DataSetSessionWindowAggregateCombineGroupFunction(
    aggregates: Array[Aggregate[_ <: Any]],
    groupingKeys: Array[Int],
    intermediateRowArity: Int,
    gap:Long,
    @transient returnType: TypeInformation[Row])
  extends RichGroupCombineFunction[Row,Row]
  with ResultTypeQueryable[Row] {

  private var aggregateBuffer: Row = _
  private var rowTimePos = 0

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupingKeys)
    aggregateBuffer = new Row(intermediateRowArity)
    rowTimePos = intermediateRowArity - 2
  }

  /**
    * For sub-grouped intermediate aggregate Rows, dividing  window according to the row-time
    * and merge data within a unified window into aggregate buffer.
    * The algorithm of dividing window is  current'row-time -  previous’row-time > gap.
    *
    * @param records  Sub-grouped intermediate aggregate Rows iterator.
    * @return Combined intermediate aggregate Row.
    *
    */
  override def combine(
    records: Iterable[Row],
    out: Collector[Row]): Unit = {

    var head:Row = null
    var lastRowTime: Option[Long] = None
    var currentRowTime: Option[Long] = None

    records.foreach(
      (record) => {
        currentRowTime = Some(record.productElement(rowTimePos).asInstanceOf[Long])

        // Initial traversal or new window open
        // Current session window end is last row-time + gap
        if (lastRowTime == None ||
          (lastRowTime != None && (currentRowTime.get > (lastRowTime.get + gap)))) {

          // Calculate the current window and open a new window
          if (lastRowTime != None) {
            // Emit the current window's merged data
            doCollect(out, head, lastRowTime.get)
          }

          // Initiate intermediate aggregate value.
          aggregates.foreach(_.initiate(aggregateBuffer))
          head = record
        }

        // Merge intermediate aggregate value to buffer.
        aggregates.foreach(_.merge(record, aggregateBuffer))

        //The current row-time is the last row-time of the next calculation
        lastRowTime = currentRowTime
      })
    // Emit the current window's merged data
    doCollect(out, head, lastRowTime.get)
  }

  def doCollect(
    out: Collector[Row],
    head: Row,
    lastRowTime: Long): Unit = {

    // Set group keys to aggregateBuffer.
    for (i <- 0 until groupingKeys.length) {
      aggregateBuffer.setField(i, head.productElement(i))
    }

    //The window's start attribute value is the min (row-time) of all rows in the window
    val windowStart = head.productElement(rowTimePos).asInstanceOf[Long]

    //The window's end property value is max (row-time) + gap for all rows in the window
    val windowEnd = lastRowTime + gap

    //intermediate Row WindowStartPos is row-time pos
    aggregateBuffer.setField(rowTimePos, windowStart)
    //intermediate Row WindowEndPos is row-time pos + 1
    aggregateBuffer.setField(rowTimePos + 1, windowEnd)

    out.collect(aggregateBuffer)
  }

  override def getProducedType: TypeInformation[Row] = {
    returnType
  }
}
