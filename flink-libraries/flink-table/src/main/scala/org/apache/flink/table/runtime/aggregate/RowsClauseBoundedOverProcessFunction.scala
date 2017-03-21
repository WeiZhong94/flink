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

import java.lang.{Long => JLong}
import java.util.{ArrayList, List => JList}

import org.apache.flink.api.common.state._
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}
import org.apache.flink.table.TableAPIConfigConstant._

import scala.collection.mutable
import scala.collection.mutable.PriorityQueue

/**
  * Process Function for ROWS clause bounded OVER window
  *
  * @param aggregates           the list of all [[org.apache.flink.table.functions.AggregateFunction]]
  *                             used for this aggregation
  * @param aggFields            the position (in the input Row) of the input value for each aggregate
  * @param forwardedFieldCount  the count of forwarded fields.
  * @param aggregationStateType the row type info of aggregation
  * @param inputType            the row type info for input data
  * @param precedingOffset      the preceding offset
  * @param isRowTimeType        It is a tag that indicates whether the time type is rowTimeType
  */
class RowsClauseBoundedOverProcessFunction(
    private val aggregates: Array[AggregateFunction[_]],
    private val aggFields: Array[Int],
    private val forwardedFieldCount: Int,
    private val aggregationStateType: RowTypeInfo,
    private val inputType: RowTypeInfo,
    private val precedingOffset: Int,
    private val isRowTimeType: Boolean)
  extends ProcessFunction[Row, Row] with CheckpointedFunction {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)

  private var output: Row = _
  private var accumulators: Row = _

  // the intermediate buffer which keeps the last expired row
  private var lastExpiredRow: Row = _
  // the state which keeps the last expired data for failover recovery
  private var lastExpiredRowState: ListState[Row] = null

  // a priority queue which keeps the time stamps of the data that are not expired for the case of
  // event-time. The first element of the tuple is the origin time stamp (ts) of each data.
  // The second element of the tuple is the derived time stamp (Long.MaxValue - ts), as we want
  // a min-priority queue based on ts.This timeStampPriorityQueue is used for retracting the expired
  // (out of window scope) data while the over window is sliding.
  private var timestampPriorityQueue: PriorityQueue[(Long, Long)] = _

  // a list which keeps the time stamps for the case of proc-time
  private var timestampList: JList[Long] = _

  // the state which is used to materialize the time stamps. For event-time processing,
  // it saves the timestampPriorityQueue, while for proc-time, it saves timestampList.
  private var timestampState: ListState[Long] = null

  // the state which used to materialize the accumulator for incremental calculation
  private var accumulatorState: ValueState[Row] = _

  // the state which keeps all the data that are not expired.
  // The first element (as the mapState key) of the tuple is the time stamp. Per each time stamp,
  // the second element of tuple is a list that contains the entire data of all the rows belonging
  // to this time stamp.
  private var dataState: MapState[Long, JList[Row]] = _

  private var lastTriggerTimestamp: JLong = JLong.MIN_VALUE

  private var allowedLateness = 0L

  override def open(config: Configuration) {

    val jobParameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters
    if (null != jobParameters) {
      // OVER_EVENT_TIME_ALLOWED_LATENESS is applied for entire job. Ideally, we want to define
      // OVER_EVENT_TIME_ALLOWED_LATENESS for each "OVER" window.
      val allowedLatenessStr = jobParameters.toMap.get(OVER_EVENT_TIME_ALLOWED_LATENESS)
      if (null != allowedLatenessStr)
        allowedLateness = allowedLatenessStr.toLong
    }

    output = new Row(forwardedFieldCount + aggregates.length)

    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("overState", aggregationStateType)
    accumulatorState = getRuntimeContext.getState(stateDescriptor)

    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]](
        "overMapState",
        classOf[Long],
        classOf[JList[Row]])

    dataState = getRuntimeContext.getMapState(mapStateDescriptor)

    if (isRowTimeType) {
      implicit val ord: Ordering[(Long, Long)] = Ordering.by(_._2)
      timestampPriorityQueue = new mutable.PriorityQueue[(Long, Long)]()
    } else {
      timestampList = new ArrayList[Long]()
    }
  }

  override def processElement(
      input: Row,
      ctx: ProcessFunction[Row, Row]#Context,
      out: Collector[Row]): Unit = {

    if (isRowTimeType) {
      // triggering timestamp for trigger calculation
      val triggeringTs = ctx.timestamp + allowedLateness
      // check if the data is expired, if not, save the data and register event time timer
      if (triggeringTs > lastTriggerTimestamp
          && triggeringTs > ctx.timerService.currentWatermark()) {
        if (dataState.contains(triggeringTs)) {
          val data = dataState.get(triggeringTs)
          data.add(input)
          dataState.put(triggeringTs, data)
        } else {
          val data = new ArrayList[Row]()
          data.add(input)
          dataState.put(triggeringTs, data)
          // register event time timer
          ctx.timerService().registerEventTimeTimer(triggeringTs)
        }
      }
    } else {
      val processingTimestamp = ctx.timerService().currentProcessingTime()

      if (dataState.contains(processingTimestamp)) {
        val data = dataState.get(processingTimestamp)
        data.add(input)
        dataState.put(processingTimestamp, data)
      } else {
        val data = new ArrayList[Row]()
        data.add(input)
        dataState.put(processingTimestamp, data)
      }
      process(processingTimestamp, input, out)
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: ProcessFunction[Row, Row]#OnTimerContext,
      out: Collector[Row]): Unit = {
    // gets all window data from state for the calculation
    val inputs: JList[Row] = dataState.get(timestamp)
    if (null != inputs) {
      var j: Int = 0
      while (j < inputs.size) {
        val input = inputs.get(j)
        process(timestamp, input, out)
        j = j + 1
      }
    }
    lastTriggerTimestamp = timestamp
  }

  /**
    * Processing the current element
    *
    * @param timestamp timestamp of current element
    * @param input     the element to be calculated
    * @param out       the collector of the results
    */
  def process(timestamp: Long, input: Row, out: Collector[Row]): Unit = {
    accumulators = accumulatorState.value()

    // initialize when first run or failover recovery per key
    if (null == accumulators) {
      lastExpiredRow = null
      val lastValueOperatorIt = lastExpiredRowState.get().iterator()
      if (lastValueOperatorIt.hasNext) {
        lastExpiredRow = lastValueOperatorIt.next()
      }
      if (isRowTimeType) {
        timestampPriorityQueue.clear()
      } else {
        timestampList.clear()
      }
      accumulators = new Row(aggregates.length)
      var i = 0
      while (i < aggregates.length) {
        accumulators.setField(i, aggregates(i).createAccumulator())
        i += 1
      }
    }

    // initialize the data from the state
    if (isRowTimeType) {
      if (timestampPriorityQueue.isEmpty) {
        val bufferPriorityIt = timestampState.get().iterator()
        while (bufferPriorityIt.hasNext) {
          val ts = bufferPriorityIt.next()
          timestampPriorityQueue.enqueue((ts, Long.MaxValue - ts))
        }
      }
    } else {
      if (timestampList.isEmpty) {
        val bufferPriorityIt = timestampState.get().iterator()
        while (bufferPriorityIt.hasNext) {
          val ts = bufferPriorityIt.next()
          timestampList.add(ts)
        }
      }
    }

    if (isRowTimeType) {
      // add the timestamp to the priority-queue
      timestampPriorityQueue.enqueue((timestamp, Long.MaxValue - timestamp))
      if (timestampPriorityQueue.size > precedingOffset) {
        val expiredRowTimestamp = timestampPriorityQueue.dequeue()
        val windowDataList = dataState.get(expiredRowTimestamp._1)

        lastExpiredRow = windowDataList.get(0)
        windowDataList.remove(0)

        if (windowDataList.size() > 0) {
          dataState.put(expiredRowTimestamp._1, windowDataList)
        } else {
          dataState.remove(expiredRowTimestamp._1)
        }
      }
    } else {
      // add the timestamp to the List
      timestampList.add(timestamp)
      if (timestampList.size >= precedingOffset) {
        val expiredRowTimestamp = timestampList.get(0)
        timestampList.remove(0)
        val windowDataList = dataState.get(expiredRowTimestamp)

        lastExpiredRow = windowDataList.get(0)

        windowDataList.remove(0)

        if (windowDataList.size() > 0) {
          dataState.put(expiredRowTimestamp, windowDataList)
        } else {
          dataState.remove(expiredRowTimestamp)
        }
      }
    }

    var i = 0
    while (i < forwardedFieldCount) {
      output.setField(i, input.getField(i))
      i += 1
    }

    if (null != lastExpiredRow) {
      i = 0
      while (i < aggregates.length) {
        val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
        val agg = aggregates(i)
        aggregates(i).retract(accumulator, lastExpiredRow.getField(aggFields(i)))
        i += 1
      }
    }

    i = 0
    while (i < aggregates.length) {
      val index = forwardedFieldCount + i
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
      aggregates(i).accumulate(accumulator, input.getField(aggFields(i)))
      output.setField(index, aggregates(i).getValue(accumulator))
      i += 1
    }
    accumulatorState.update(accumulators)

    out.collect(output)
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    accumulatorState.clear()
    accumulatorState.update(accumulators)

    lastExpiredRowState.clear()
    lastExpiredRowState.add(lastExpiredRow)

    timestampState.clear()
    if (isRowTimeType) {
      while (timestampPriorityQueue.nonEmpty) {
        timestampState.add(timestampPriorityQueue.dequeue()._1)
      }
    } else {
      var i = 0
      while (i < timestampList.size()) {
        timestampState.add(timestampList.get(i))
        i = i + 1
      }
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val LastValueOperatorStateSerializer = inputType.createSerializer(getRuntimeContext.getExecutionConfig)
    val LastValueOperatorDescriptor =
      new ListStateDescriptor[Row]("lastValueOperatorState", LastValueOperatorStateSerializer)
    lastExpiredRowState =
        context.getOperatorStateStore.getOperatorState(LastValueOperatorDescriptor)
    val bufferPriorityQueueOperatorDescriptor =
      new ListStateDescriptor[Long]("bufferPriorityQueueOOperatorState", classOf[Long])
    timestampState =
        context.getOperatorStateStore.getOperatorState(bufferPriorityQueueOperatorDescriptor)
  }

}
