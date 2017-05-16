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
package org.apache.flink.table.runtime.triggers

import java.lang.{Long => JLong}

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.table.api.{StreamQueryConfig, Types}
import org.apache.flink.table.runtime.triggers.CountTriggerWithCleanupState.Sum

class CountTriggerWithCleanupState[W <: Window](queryConfig: StreamQueryConfig, maxCount: Long)
  extends Trigger[Any, W] {

  private val serialVersionUID: Long = 1L

  protected val minRetentionTime: Long = queryConfig.getMinIdleStateRetentionTime
  protected val maxRetentionTime: Long = queryConfig.getMaxIdleStateRetentionTime
  protected val stateCleaningEnabled: Boolean = minRetentionTime > 1

  private val stateDesc: ReducingStateDescriptor[JLong] =
    new ReducingStateDescriptor[JLong]("count", new Sum, Types.LONG)

  private val cleanupStateDesc: ValueStateDescriptor[JLong] =
    new ValueStateDescriptor[JLong]("countCleanup", Types.LONG)

  override def canMerge: Boolean = true

  override def onMerge(window: W, ctx: Trigger.OnMergeContext) {
    ctx.mergePartitionedState(stateDesc)
  }

  override def toString: String = "CountTriggerWithCleanupState(" +
    "minIdleStateRetentionTime=" + queryConfig.getMinIdleStateRetentionTime + ", " +
    "maxIdleStateRetentionTime=" + queryConfig.getMaxIdleStateRetentionTime + ", " +
    "maxCount=" + maxCount + ")"

  override def onElement(
      element: Any,
      timestamp: Long,
      window: W,
      ctx: TriggerContext): TriggerResult = {
    println(toString)

    val currentTime = ctx.getCurrentProcessingTime

    if (stateCleaningEnabled) {
      // last registered timer
      val curCleanupTime = ctx.getPartitionedState(cleanupStateDesc).value()

      // check if a cleanup timer is registered and
      // that the current cleanup timer won't delete state we need to keep
      if (curCleanupTime == null || (currentTime + minRetentionTime) > curCleanupTime) {
        // we need to register a new (later) timer
        val cleanupTime = currentTime + maxRetentionTime
        // register timer and remember clean-up time
        ctx.registerProcessingTimeTimer(cleanupTime)

        if (null != curCleanupTime) {
          ctx.deleteProcessingTimeTimer(curCleanupTime)
        }

        ctx.getPartitionedState(cleanupStateDesc).update(cleanupTime)

        println(
          "currentTime[" + currentTime + "] curCleanupTime=[" + curCleanupTime + "] register=>[" +
            cleanupTime + "]")
      }
    }

    val count: ReducingState[JLong] = ctx.getPartitionedState(stateDesc)
    count.add(1L)
    if (count.get >= maxCount) {
      count.clear()
      return TriggerResult.FIRE
    }
    return TriggerResult.CONTINUE

  }

  override def onProcessingTime(
      time: Long,
      window: W,
      ctx: TriggerContext): TriggerResult = {
    println("trigger[" + time + "]")
    if (stateCleaningEnabled) {
      val cleanupTime = ctx.getPartitionedState(cleanupStateDesc).value()
      // check that the triggered timer is the last registered processing time timer.
      if (null != cleanupTime && time == cleanupTime) {
        println("onProcessingTime=>[" + time + "] FIRE_AND_PURGE")
        clear(window, ctx)
        TriggerResult.FIRE_AND_PURGE
      } else {
        TriggerResult.CONTINUE
      }
    } else {
      TriggerResult.CONTINUE
    }
  }

  override def onEventTime(
      time: Long,
      window: W,
      ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(
      window: W,
      ctx: TriggerContext): Unit = {
    println("Clean state")
    ctx.getPartitionedState(stateDesc).clear()
    ctx.getPartitionedState(cleanupStateDesc).clear()
  }

}

object CountTriggerWithCleanupState {

  /**
    *
    * @param maxCount The count of elements at which to fire.
    * @tparam W The type of { @link Window Windows} on which this trigger can operate.
    */
  def of[W <: Window](
      queryConfig: StreamQueryConfig,
      maxCount: Long): CountTriggerWithCleanupState[W] =
    new CountTriggerWithCleanupState[W](queryConfig, maxCount)

  class Sum extends ReduceFunction[JLong] {
    override def reduce(
        value1: JLong,
        value2: JLong): JLong = value1 + value2
  }

}
