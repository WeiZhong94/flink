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

package org.apache.flink.table.api

import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo}
import org.apache.flink.table.api.scala.{CURRENT_RANGE, CURRENT_ROW}

/**
  * Over window is similar to the traditional OVER SQL.
  */
case class OverWindow(
    private[flink] val alias: Expression,
    private[flink] val partitionBy: Seq[Expression],
    private[flink] val orderBy: Expression,
    private[flink] val preceding: Expression,
    private[flink] val following: Expression)

case class CurrentRow() extends Expression {
  override private[flink] def resultType = RowIntervalTypeInfo.INTERVAL_ROWS

  override private[flink] def children = Seq()

  override def toString = "CURRENT ROW"
}

case class CurrentRange() extends Expression {
  override private[flink] def resultType = TimeIntervalTypeInfo.INTERVAL_MILLIS

  override private[flink] def children = Seq()

  override def toString = "CURRENT RANGE"
}

case class UnboundedRow() extends Expression {
  override private[flink] def resultType = RowIntervalTypeInfo.INTERVAL_ROWS

  override private[flink] def children = Seq()

  override def toString = "UNBOUNDED ROW"
}

case class UnboundedRange() extends Expression {
  override private[flink] def resultType = TimeIntervalTypeInfo.INTERVAL_MILLIS

  override private[flink] def children = Seq()

  override def toString = "UNBOUNDED RANGE"
}

/**
  * An over window predefined specification.
  */
class OverWindowPredefined(
    private val partitionBy: Seq[Expression],
    private val orderBy: Expression) {

  private[flink] var preceding: Expression = _
  private[flink] var following: Expression = _

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return over window
    */
  def as(alias: String): OverWindow = as(ExpressionParser.parseExpression(alias))

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return over window
    */
  def as(alias: Expression): OverWindow = {

    // set following to CURRENT_ROW / CURRENT_RANGE if not defined
    if (null == following) {
      if (preceding.resultType.isInstanceOf[RowIntervalTypeInfo]) {
        following = CURRENT_ROW
      } else {
        following = CURRENT_RANGE
      }
    }
    OverWindow(alias, partitionBy, orderBy, preceding, following)
  }

  /**
    * Set the preceding offset (based on time or row-count intervals) for over window.
    *
    * @param preceding preceding offset relative to the current row.
    * @return this over window
    */
  def preceding(preceding: String): OverWindowPredefined = {
    this.preceding(ExpressionParser.parseExpression(preceding))
  }

  /**
    * Set the preceding offset (based on time or row-count intervals) for over window.
    *
    * @param preceding preceding offset relative to the current row.
    * @return this over window
    */
  def preceding(preceding: Expression): OverWindowPredefined = {
    this.preceding = preceding
    this
  }

  /**
    * Set the following offset (based on time or row-count intervals) for over window.
    *
    * @param following following offset that relative to the current row.
    * @return this over window
    */
  def following(following: String): OverWindowPredefined = {
    this.following(ExpressionParser.parseExpression(following))
  }

  /**
    * Set the following offset (based on time or row-count intervals) for over window.
    *
    * @param following following offset that relative to the current row.
    * @return this over window
    */
  def following(following: Expression): OverWindowPredefined = {
    this.following = following
    this
  }
}

/**
  * A window specification.
  *
  * Window groups rows based on time or row-count intervals. It is a general way to group the
  * elements, which is very helpful for both groupby-aggregations and over-aggregations to
  * compute aggregates on groups of elements.
  *
  * Infinite streaming tables can only be grouped into time or row intervals. Hence window grouping
  * is required to apply aggregations on streaming tables.
  *
  * For finite batch tables, window provides shortcuts for time-based groupBy.
  *
  * @param alias The expression of alias for this Window
  */
abstract class Window(val alias: Expression) {

  /**
    * Converts an API class to a logical window for planning.
    */
  private[flink] def toLogicalWindow: LogicalWindow
}

/**
  * A predefined specification of window
  */
abstract class WindowPredefined {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: Expression): Window

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: String): Window = as(ExpressionParser.parseExpression(alias))
}

/**
  * A predefined specification of window on processing-time
  */
abstract class ProcessingTimeWindowPredefined extends WindowPredefined {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a predefined window on event-time
    */
  def on(timeField: Expression): WindowPredefined

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a predefined window on event-time
    */
  def on(timeField: String): WindowPredefined =
    on(ExpressionParser.parseExpression(timeField))
}

/**
  * A window operating on event-time.
  *
  * For streaming tables call on('rowtime) to specify grouping by event-time.
  * Otherwise rows are grouped by processing-time.
  *
  * For batch tables, refer to a timestamp or long attribute.
  *
  * @param timeField time mode for streaming tables and time attribute for batch tables
  */
abstract class EventTimeWindow(alias: Expression, val timeField: Expression) extends Window(alias)

// ------------------------------------------------------------------------------------------------
// Tumbling windows
// ------------------------------------------------------------------------------------------------

/**
  * A predefined specification of tumbling window on processing-time.
  *
  * @param size the size of the window either a time or a row-count interval.
  */
class TumblingWindowPredefined(size: Expression) extends ProcessingTimeWindowPredefined {

  def this(size: String) = this(ExpressionParser.parseExpression(size))

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time.
    * Otherwise rows are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a predefined window on event-time
    */
  override def on(timeField: Expression): WindowPredefined =
    new TumblingEventTimeWindowPreDefined(timeField, size)

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  override def as(alias: Expression) = new TumblingWindow(alias, size)
}

/**
  * Tumbling window.
  *
  * grouped by processing-time.
  *
  * @param size the size of the window either a time or a row-count interval.
  */
class TumblingWindow(alias: Expression, size: Expression) extends Window(alias) {

  override private[flink] def toLogicalWindow: LogicalWindow =
    ProcessingTimeTumblingGroupWindow(alias, size)
}

/**
  * A predefined specification of tumbling window on event-time.
  */
class TumblingEventTimeWindowPreDefined(
    time: Expression,
    size: Expression) extends WindowPredefined {
  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  override def as(alias: Expression): Window = new TumblingEventTimeWindow(alias, time, size)
}

/**
  * Tumbling window on event-time.
  */
class TumblingEventTimeWindow(
    alias: Expression,
    time: Expression,
    size: Expression) extends EventTimeWindow(alias, time) {

  override private[flink] def toLogicalWindow: LogicalWindow =
    EventTimeTumblingGroupWindow(alias, time, size)
}

// ------------------------------------------------------------------------------------------------
// Sliding windows
// ------------------------------------------------------------------------------------------------

/**
  * Partially specified sliding window.
  *
  * @param size the size of the window either a time or a row-count interval.
  */
class SlideWithSize(size: Expression) {

  /**
    * Partially specified sliding window.
    *
    * @param size the size of the window either a time or a row-count interval.
    */
  def this(size: String) = this(ExpressionParser.parseExpression(size))

  /**
    * Specifies the window's slide as time or row-count interval.
    *
    * The slide determines the interval in which windows are started. Hence, sliding windows can
    * overlap if the slide is smaller than the size of the window.
    *
    * For example, you could have windows of size 15 minutes that slide by 3 minutes. With this
    * 15 minutes worth of elements are grouped every 3 minutes and each row contributes to 5
    * windows.
    *
    * @param slide the slide of the window either as time or row-count interval.
    * @return a predefined sliding window.
    */
  def every(slide: Expression): SlidingWindowPreDefined = new SlidingWindowPreDefined(size, slide)

  /**
    * Specifies the window's slide as time or row-count interval.
    *
    * The slide determines the interval in which windows are started. Hence, sliding windows can
    * overlap if the slide is smaller than the size of the window.
    *
    * For example, you could have windows of size 15 minutes that slide by 3 minutes. With this
    * 15 minutes worth of elements are grouped every 3 minutes and each row contributes to 5
    * windows.
    *
    * @param slide the slide of the window either as time or row-count interval.
    * @return a predefined sliding window.
    */
  def every(slide: String): WindowPredefined = every(ExpressionParser.parseExpression(slide))
}

/**
  * A predefined specification of sliding window on processing-time.
  */
class SlidingWindowPreDefined(
    size: Expression,
    slide: Expression) extends ProcessingTimeWindowPredefined {
  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return a predefined Sliding window on event-time.
    */
  override def on(timeField: Expression): SlidingEventTimeWindowPreDefined =
    new SlidingEventTimeWindowPreDefined(timeField, size, slide)

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  override def as(alias: Expression): Window = new SlidingWindow(alias, size, slide)
}

/**
  * Sliding window.
  *
  * grouped by processing-time.
  *
  * @param size the size of the window either a time or a row-count interval.
  */
class SlidingWindow(
    alias: Expression,
    size: Expression,
    slide: Expression)
  extends Window(alias) {

  override private[flink] def toLogicalWindow: LogicalWindow =
    ProcessingTimeSlidingGroupWindow(alias, size, slide)
}

/**
  * A predefined specification of sliding window on event-time.
  */
class SlidingEventTimeWindowPreDefined(
    timeField: Expression,
    size: Expression,
    slide: Expression) extends WindowPredefined {
  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  override def as(alias: Expression): Window =
    new SlidingEventTimeWindow(alias, timeField, size, slide)
}

/**
  * Sliding window on event-time.
  */
class SlidingEventTimeWindow(
    alias: Expression,
    timeField: Expression,
    size: Expression,
    slide: Expression)
  extends EventTimeWindow(alias, timeField) {

  override private[flink] def toLogicalWindow: LogicalWindow =
    EventTimeSlidingGroupWindow(alias, timeField, size, slide)
}

// ------------------------------------------------------------------------------------------------
// Session windows
// ------------------------------------------------------------------------------------------------

/**
  * A predefined specification of session window on processing-time
  */
class SessionWindowPreDefined(gap: Expression) extends ProcessingTimeWindowPredefined {

  def this(gap: String) = this(ExpressionParser.parseExpression(gap))

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[on('rowtime)]] to specify grouping by event-time. Otherwise rows
    * are grouped by processing-time.
    *
    * For batch tables, refer to a timestamp or long attribute.
    *
    * @param timeField time mode for streaming tables and time attribute for batch tables
    * @return an on event-time session window on event-time
    */
  override def on(timeField: Expression): SessionEventTimeWindowPreDefined =
    new SessionEventTimeWindowPreDefined(timeField, gap)

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  override def as(alias: Expression): Window = new SessionWindow(alias, gap)
}

/**
  * Session window.
  *
  * grouped by processing-time.
  *
  * @param gap the time interval of inactivity before a window is closed.
  */
class SessionWindow(alias: Expression, gap: Expression) extends Window(alias) {

  override private[flink] def toLogicalWindow: LogicalWindow =
    ProcessingTimeSessionGroupWindow(alias, gap)
}

/**
  * A predefined specification of window on event-time session.
  */
class SessionEventTimeWindowPreDefined(
    timeField: Expression,
    gap: Expression) extends WindowPredefined {
  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  override def as(alias: Expression): Window = new SessionEventTimeWindow(alias, timeField, gap)
}

/**
  * Session window on event-time.
  */
class SessionEventTimeWindow(
    alias: Expression,
    timeField: Expression,
    gap: Expression)
  extends EventTimeWindow(alias, timeField) {

  override private[flink] def toLogicalWindow: LogicalWindow =
    EventTimeSessionGroupWindow(alias, timeField, gap)
}
