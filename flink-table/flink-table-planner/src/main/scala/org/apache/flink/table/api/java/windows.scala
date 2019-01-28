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

package org.apache.flink.table.api.java

import org.apache.flink.table.api.{Window, _}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.plan.expressions.{ExpressionParser, PlannerExpression}

/**
  * Helper class for creating a tumbling window. Tumbling windows are consecutive, non-overlapping
  * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
  * elements in 5 minutes intervals.
  */
object Tumble {

  /**
    * Creates a tumbling window. Tumbling windows are consecutive, non-overlapping
    * windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups
    * elements in 5 minutes intervals.
    *
    * @param size the size of the window as time or row-count interval.
    * @return a partially defined tumbling window
    */
  def over(size: String): JavaTumbleWithSize = new JavaTumbleWithSize(size)
}

/**
  * Helper class for creating a sliding window. Sliding windows have a fixed size and slide by
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
    * consecutive window evaluations.
    *
    * @param size the size of the window as time or row-count interval
    * @return a partially specified sliding window
    */
  def over(size: String): JavaSlideWithSize = new JavaSlideWithSize(size)
}

/**
  * Helper class for creating a session window. The boundary of session windows are defined by
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
  def withGap(gap: String): JavaSessionWithGap = new JavaSessionWithGap(gap)
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
  def orderBy(orderBy: String): JavaOverWindowWithOrderBy = {
    JavaOverWindowWithOrderBy("", orderBy)
  }

  /**
    * Partitions the elements on some partition keys.
    *
    * @param partitionBy some partition keys.
    * @return A partitionedOver instance that only contains the orderBy method.
    */
  def partitionBy(partitionBy: String): JavaPartitionedOver = {
    JavaPartitionedOver(partitionBy)
  }
}

case class JavaPartitionedOver(partitionBy: String) {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[orderBy 'rowtime or orderBy 'proctime]] to specify time mode.
    *
    * For batch tables, refer to a timestamp or long attribute.
    */
  def orderBy(orderBy: String): JavaOverWindowWithOrderBy = {
    JavaOverWindowWithOrderBy(partitionBy, orderBy)
  }
}


case class JavaOverWindowWithOrderBy(partitionBy: String, orderBy: String) {

  /**
    * Set the preceding offset (based on time or row-count intervals) for over window.
    *
    * @param preceding preceding offset relative to the current row.
    * @return this over window
    */
  def preceding(preceding: String): JavaOverWindowWithPreceding = {
    new JavaOverWindowWithPreceding(partitionBy, orderBy, preceding)
  }

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return over window
    */
  def as(alias: String): JavaOverWindow = {
    JavaOverWindow(alias, partitionBy, orderBy, "unbounded_range", "current_range")
  }
}

/**
  * A partially defined over window.
  */
class JavaOverWindowWithPreceding(
    private val partitionBy: String,
    private val orderBy: String,
    private val preceding: String) {

  private[flink] var following: String = _

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return over window
    */
  def as(alias: String): JavaOverWindow = {
    JavaOverWindow(alias, partitionBy, orderBy, preceding, following)
  }

  /**
    * Set the following offset (based on time or row-count intervals) for over window.
    *
    * @param following following offset that relative to the current row.
    * @return this over window
    */
  def following(following: String): JavaOverWindowWithPreceding = {
    this.following = following
    this
  }
}

/**
  * Over window is similar to the traditional OVER SQL.
  */
case class JavaOverWindow(
    private[flink] val alias: String,
    private[flink] val partitionBy: String,
    private[flink] val orderBy: String,
    private[flink] val preceding: String,
    private[flink] val following: String) extends UnresolvedOverWindow

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
class JavaTumbleWithSize(size: String) {

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
  def on(timeField: String): JavaTumbleWithSizeOnTime =
    new JavaTumbleWithSizeOnTime(timeField, size)
}

/**
  * Tumbling window on time.
  */
class JavaTumbleWithSizeOnTime(time: String, size: String) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: String): JavaTumbleWithSizeOnTimeWithAlias = {
    new JavaTumbleWithSizeOnTimeWithAlias(alias, time, size)
  }
}

/**
  * Tumbling window on time with alias. Fully specifies a window.
  */
case class JavaTumbleWithSizeOnTimeWithAlias(
    alias: String,
    timeField: String,
    size: String) extends Window

// ------------------------------------------------------------------------------------------------
// Sliding windows
// ------------------------------------------------------------------------------------------------


/**
  * Partially specified sliding window.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class JavaSlideWithSize(size: String) {

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
  def every(slide: String): JavaSlideWithSizeAndSlide =
    new JavaSlideWithSizeAndSlide(size, slide)
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
class JavaSlideWithSizeAndSlide(size: String, slide: String) {

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
  def on(timeField: String): JavaSlideWithSizeAndSlideOnTime =
    new JavaSlideWithSizeAndSlideOnTime(timeField, size, slide)
}

/**
  * Sliding window on time.
  */
class JavaSlideWithSizeAndSlideOnTime(
    timeField: String,
    size: String,
    slide: String) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: String): JavaSlideWithSizeAndSlideOnTimeWithAlias = {
    JavaSlideWithSizeAndSlideOnTimeWithAlias(alias, timeField, size, slide)
  }
}

/**
  * Sliding window on time with alias. Fully specifies a window.
  */
case class JavaSlideWithSizeAndSlideOnTimeWithAlias(
    alias: String,
    timeField: String,
    size: String,
    slide: String) extends Window

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
class JavaSessionWithGap(gap: String) {

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
  def on(timeField: String): JavaSessionWithGapOnTime =
    new JavaSessionWithGapOnTime(timeField, gap)
}

/**
  * Session window on time.
  */
class JavaSessionWithGapOnTime(timeField: String, gap: String) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: String): JavaSessionWithGapOnTimeWithAlias = {
    JavaSessionWithGapOnTimeWithAlias(alias, timeField, gap)
  }
}

/**
  * Session window on time with alias. Fully specifies a window.
  */
case class JavaSessionWithGapOnTimeWithAlias(
    alias: String,
    timeField: String,
    gap: String) extends Window
