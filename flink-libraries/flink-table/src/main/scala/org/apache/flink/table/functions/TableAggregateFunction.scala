package org.apache.flink.table.functions

import org.apache.flink.api.common.typeinfo.TypeInformation


/**
  * Base class for User-Defined table Aggregates.
  *
  * <p>The behavior of an {@link TableAggregateFunction} can be defined by implementing
  * a series of custom methods. An {@link TableAggregateFunction} needs at least three methods:
  *  - createAccumulator,
  *  - accumulate, and
  *  - emitValue or emitValueWithRetract
  *
  * <p>There are a few other methods that can be optional to have:
  *  - retract,
  *  - merge
  *
  * <p>All these methods muse be declared publicly, not static and named exactly as the names
  * mentioned above. The methods createAccumulator and emitValue are defined in the
  * {@link TableAggregateFunction} functions, while other methods are explained below.
  *
  *
  * {@code
  * Processes the input values and update the provided accumulator instance. The method
 * accumulate can be overloaded with different custom types and arguments. An AggregateFunction
 * requires at least one accumulate() method.
 *
 * param accumulator the accumulator which contains the current aggregated results
 * param [user defined inputs] the input value (usually obtained from a new arrived data).
 *
 * public void accumulate(ACC accumulator, [user defined inputs])
 * }
  *
  *
  * {@code
  * Retracts the input values from the accumulator instance. The current design assumes the
 * inputs are the values that have been previously accumulated. The method retract can be
 * overloaded with different custom types and arguments. This function must be implemented for
 * datastream bounded over aggregate.
 *
 * param accumulator   the accumulator which contains the current aggregated results
 * param [user defined inputs] the input value (usually obtained from a new arrived data).aol
 *
 * public void retract(ACC accumulator, [user defined inputs])
 * }
  *
  *
  * {@code
  * Merges a group of accumulator instances into one accumulator instance. This function must be
 * implemented for datastream session window grouping aggregate and dataset grouping aggregate.
 *
 * param accumulator  the accumulator which will keep the merged aggregate results. It should
 *                    be noted that the accumulator may contain the previous aggregated
 *                    results. Therefore user should not replace or clean this instance in the
 *                    custom merge method.
 * param its  an {@link Iterable} pointed to a group of accumulators that will be
  * merged.
  *
  * public void merge(ACC accumulator, Iterable<ACC> its)
  * }
  *
  *
  * {@code
  * Called every time when an table-valued aggregation result should be materialized.
 * The returned value could be either an early and incomplete result
 * (periodically emitted as data arrive) or the final result of the table-valued aggregation.
 *
 * The implementation logic do not need deal with retract messages.
 * For Example, if we calculate top3(ASC order), the behavior as follows:
 * Assume there are 4 messages: {1, 2, 6, 4}
  * - Framework should generate retract message according to DAG pattern.
  * -----------------------------------------------------------------------------------------------
  * | Input   |  OutPut              | Framework  behavior of when need retract message           |
  * -----------------------------------------------------------------------------------------------
  * | 1       | collector.collect(1) |                                                            |
  * -----------------------------------------------------------------------------------------------
  * | 2       | collector.collect(1) | collector.retract(1)                                       |
  * |         | collector.collect(2) |                                                            |
  * -----------------------------------------------------------------------------------------------
  * | 6       | collector.collect(1) | collector.retract(1)                                       |
  * |         | collector.collect(2) | collector.retract(2)                                       |
  * |         | collector.collect(6) |                                                            |
  * -----------------------------------------------------------------------------------------------
  * | 4       | collector.collect(1) | collector.retract(1)                                       |
  * |         | collector.collect(2) | collector.retract(2)                                       |
  * |         | collector.collect(4) | collector.retract(6)                                       |
  * -----------------------------------------------------------------------------------------------
  *
  * public void emitValue(ACC accumulator, Collector<T> out)
  * }
  *
  *
  * {@code
  * Called every time when an table-valued aggregation result should be materialized.
 * The returned value could be either an early and incomplete result
 * (periodically emitted as data arrive) or the final result of the table-valued aggregation.
 *
 *
 * The implementation logic should deal with retract message.
 * For Example, if we calculate top3(ASC order), the behavior as follows:
 * Assume there are 4 messages: {1, 2, 6, 4}
  * - Framework do not need generate the retract message.
  * --------------------------------
  * | Input |        OutPut        |
  * |------------------------------|
  * | 1     | collector.collect(1) |
  * |------------------------------|
  * | 2     | collector.collect(2) |
  * |------------------------------|
  * | 6     | collector.collect(6) |
  * |------------------------------|
  * | 4     | collector.retract(6) |
  * |       | collector.collect(4) |
  * --------------------------------
  *
  * public void emitValueWithRetract(ACC accumulator, Collector<T> out)
  * }
  *
  * @param < T>   the type of the table aggregation result
  * @param < ACC> the type of the table aggregation accumulator. The accumulator is used to keep the
  *          table aggregated values which are needed to compute an table aggregation result.
  *          TableAggregateFunction represents its state using accumulator, thereby the state of the
  *          TableAggregateFunction must be put into the accumulator.
  */
abstract class TableAggregateFunction[T, ACC] extends AccumulateFunction[T, ACC] {}
