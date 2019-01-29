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

import org.apache.flink.table.api._
import org.apache.flink.table.expressions.{Expression, LeafExpression}

/**
  * Helper object for creating a tumbling window. Tumbling windows are consecutive, non-overlapping
  * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
  * elements in 5 minutes intervals.
  */
object Tumble {

  /**
    * Creates a tumbling window. Tumbling windows are fixed-size, consecutive, non-overlapping
    * windows. For example, a tumbling window of 5 minutes size groups
    * elements in 5 minutes intervals.
    *
    * @param size the size of the window as time or row-count interval.
    * @return a partially defined tumbling window
    */
  def over(size: Expression): ApiTumbleWithSize = new ApiTumbleWithSize(size)
}


/**
  * Helper object for creating a sliding window. Sliding windows have a fixed size and slide by
  * a specified slide interval. If the slide interval is smaller than the window size, sliding
  * windows are overlapping. Thus, an element can be assigned to multiple windows.
  *
  * For example, a sliding window of size 15 minutes with 5 minutes sliding interval groups elements
  * of 15 minutes and evaluates every five minutes. Each element is contained in three consecutive
  * window evaluations.
  */
object Slide {

  /**
    * Creates a sliding window. Sliding windows have a fixed size and slide by
    * a specified slide interval. If the slide interval is smaller than the window size, sliding
    * windows are overlapping. Thus, an element can be assigned to multiple windows.
    *
    * For example, a sliding window of size 15 minutes with 5 minutes sliding interval groups
    * elements of 15 minutes and evaluates every five minutes. Each element is contained in three
    * consecutive
    *
    * @param size the size of the window as time or row-count interval
    * @return a partially specified sliding window
    */
  def over(size: Expression): ApiSlideWithSize = new ApiSlideWithSize(size)
}

/**
  * Helper object for creating a session window. The boundary of session windows are defined by
  * intervals of inactivity, i.e., a session window is closes if no event appears for a defined
  * gap period.
  */
object Session {

  /**
    * Creates a session window. The boundary of session windows are defined by
    * intervals of inactivity, i.e., a session window is closes if no event appears for a defined
    * gap period.
    *
    * @param gap specifies how long (as interval of milliseconds) to wait for new data before
    *            closing the session window.
    * @return a partially defined session window
    */
  def withGap(gap: Expression): ApiSessionWithGap = new ApiSessionWithGap(gap)
}

/**
  * Helper object for creating a over window.
  */
object Over {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[orderBy 'rowtime or orderBy 'proctime]] to specify time mode.
    *
    * For batch tables, refer to a timestamp or long attribute.
    */
  def orderBy(orderBy: Expression): ApiOverWindowWithOrderBy = {
    new ApiOverWindowWithOrderBy(Seq[Expression](), orderBy)
  }

  /**
    * Partitions the elements on some partition keys.
    *
    * @param partitionBy some partition keys.
    * @return A partitionedOver instance that only contains the orderBy method.
    */
  def partitionBy(partitionBy: Expression*): ApiPartitionedOver = {
    ApiPartitionedOver(partitionBy.toArray)
  }
}


case class ApiPartitionedOver(partitionBy: Array[Expression]) {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[orderBy 'rowtime or orderBy 'proctime]] to specify time mode.
    *
    * For batch tables, refer to a timestamp or long attribute.
    */
  def orderBy(orderBy: Expression): ApiOverWindowWithOrderBy = {
    ApiOverWindowWithOrderBy(partitionBy, orderBy)
  }
}


case class ApiOverWindowWithOrderBy(partitionBy: Seq[Expression], orderBy: Expression) {

  /**
    * Set the preceding offset (based on time or row-count intervals) for over window.
    *
    * @param preceding preceding offset relative to the current row.
    * @return this over window
    */
  def preceding(preceding: Expression): ApiOverWindowWithPreceding = {
    new ApiOverWindowWithPreceding(partitionBy, orderBy, preceding)
  }

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return over window
    */
  def as(alias: Expression): ApiOverWindow = {
    ApiOverWindow(alias, partitionBy, orderBy, UNBOUNDED_RANGE, CURRENT_RANGE)
  }
}

/**
  * Over window is similar to the traditional OVER SQL.
  */
case class ApiOverWindow(
    private[flink] val alias: Expression,
    private[flink] val partitionBy: Seq[Expression],
    private[flink] val orderBy: Expression,
    private[flink] val preceding: Expression,
    private[flink] val following: Expression) extends UnresolvedOverWindow

case class CurrentRow() extends LeafExpression

case class CurrentRange() extends LeafExpression

case class UnboundedRow() extends LeafExpression

case class UnboundedRange() extends LeafExpression

/**
  * A partially defined over window.
  */
class ApiOverWindowWithPreceding(
    private val partitionBy: Seq[Expression],
    private val orderBy: Expression,
    private val preceding: Expression) {

  private[flink] var following: Expression = _

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return over window
    */
  def as(alias: Expression): ApiOverWindow = {
    ApiOverWindow(alias, partitionBy, orderBy, preceding, following)
  }

  /**
    * Set the following offset (based on time or row-count intervals) for over window.
    *
    * @param following following offset that relative to the current row.
    * @return this over window
    */
  def following(following: Expression): ApiOverWindowWithPreceding = {
    this.following = following
    this
  }
}

abstract class ApiWindow

// ------------------------------------------------------------------------------------------------
// Tumbling windows
// ------------------------------------------------------------------------------------------------

/**
  * Tumbling window.
  *
  * For streaming tables you can specify grouping by a event-time or processing-time attribute.
  *
  * For batch tables you can specify grouping on a timestamp or long attribute.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class ApiTumbleWithSize(size: Expression) {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables you can specify grouping by a event-time or processing-time attribute.
    *
    * For batch tables you can specify grouping on a timestamp or long attribute.
    *
    * @param timeField time attribute for streaming and batch tables
    * @return a tumbling window on event-time
    */
  def on(timeField: Expression): ApiTumbleWithSizeOnTime =
    new ApiTumbleWithSizeOnTime(timeField, size)
}

/**
  * Tumbling window on time.
  */
class ApiTumbleWithSizeOnTime(time: Expression, size: Expression) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: Expression): ApiTumbleWithSizeOnTimeWithAlias = {
    new ApiTumbleWithSizeOnTimeWithAlias(alias, time, size)
  }
}

/**
  * Tumbling window on time with alias. Fully specifies a window.
  */
case class ApiTumbleWithSizeOnTimeWithAlias(
    alias: Expression,
    timeField: Expression,
    size: Expression) extends ApiWindow

// ------------------------------------------------------------------------------------------------
// Sliding windows
// ------------------------------------------------------------------------------------------------


/**
  * Partially specified sliding window.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class ApiSlideWithSize(size: Expression) {

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
    * @return a sliding window
    */
  def every(slide: Expression): ApiSlideWithSizeAndSlide =
    new ApiSlideWithSizeAndSlide(size, slide)
}

/**
  * Sliding window.
  *
  * For streaming tables you can specify grouping by a event-time or processing-time attribute.
  *
  * For batch tables you can specify grouping on a timestamp or long attribute.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class ApiSlideWithSizeAndSlide(size: Expression, slide: Expression) {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables you can specify grouping by a event-time or processing-time attribute.
    *
    * For batch tables you can specify grouping on a timestamp or long attribute.
    *
    * @param timeField time attribute for streaming and batch tables
    * @return a tumbling window on event-time
    */
  def on(timeField: Expression): ApiSlideWithSizeAndSlideOnTime =
    new ApiSlideWithSizeAndSlideOnTime(timeField, size, slide)
}

/**
  * Sliding window on time.
  */
class ApiSlideWithSizeAndSlideOnTime(
    timeField: Expression,
    size: Expression,
    slide: Expression) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: Expression): ApiSlideWithSizeAndSlideOnTimeWithAlias = {
    ApiSlideWithSizeAndSlideOnTimeWithAlias(alias, timeField, size, slide)
  }
}

/**
  * Sliding window on time with alias. Fully specifies a window.
  */
case class ApiSlideWithSizeAndSlideOnTimeWithAlias(
    alias: Expression,
    timeField: Expression,
    size: Expression,
    slide: Expression) extends ApiWindow

// ------------------------------------------------------------------------------------------------
// Session windows
// ------------------------------------------------------------------------------------------------


/**
  * Session window.
  *
  * For streaming tables you can specify grouping by a event-time or processing-time attribute.
  *
  * For batch tables you can specify grouping on a timestamp or long attribute.
  *
  * @param gap the time interval of inactivity before a window is closed.
  */
class ApiSessionWithGap(gap: Expression) {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables you can specify grouping by a event-time or processing-time attribute.
    *
    * For batch tables you can specify grouping on a timestamp or long attribute.
    *
    * @param timeField time attribute for streaming and batch tables
    * @return a tumbling window on event-time
    */
  def on(timeField: Expression): ApiSessionWithGapOnTime =
    new ApiSessionWithGapOnTime(timeField, gap)
}

/**
  * Session window on time.
  */
class ApiSessionWithGapOnTime(timeField: Expression, gap: Expression) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: Expression): ApiSessionWithGapOnTimeWithAlias = {
    ApiSessionWithGapOnTimeWithAlias(alias, timeField, gap)
  }
}

/**
  * Session window on time with alias. Fully specifies a window.
  */
case class ApiSessionWithGapOnTimeWithAlias(
    alias: Expression,
    timeField: Expression,
    gap: Expression) extends ApiWindow
