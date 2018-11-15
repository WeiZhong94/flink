package org.apache.flink.ml.common

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Table
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction}
import org.apache.flink.api.java.tuple

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object TableFlinkMLTools {
  def block[T: TypeInformation: ClassTag](
      input: Table, numBlocks: Int, partitionerOption: Option[ScalarFunction]): Table = {
    val blockIDAssigner = new BlockIDAssigner[T](numBlocks)
    val blockIDInput = input.as('t).select(blockIDAssigner('t).flatten()).as('id, 't)

    val preGroupBlockIDInput = partitionerOption match {
      case Some(partitioner) =>
        blockIDInput.select(partitioner('id, numBlocks) as 'id, 't)

      case None => blockIDInput
    }

    val blockGenerator = new BlockGenerator[T]
    preGroupBlockIDInput.groupBy('id).select(blockGenerator('id, 't))
  }

  object ModuloKeyPartitionFunction extends ScalarFunction {
    def eval(key: Int, numPartitions: Int): Int = {
      val result = key % numPartitions

      if(result < 0) {
        result + numPartitions
      } else {
        result
      }
    }

    def eval(key: String, numPartitions: Int): Int = {
      val result = key.hashCode % numPartitions

      if (result < 0) {
        result + numPartitions
      } else {
        result
      }
    }
  }
}

class BlockIDAssigner[T: TypeInformation](numBlocks: Int) extends ScalarFunction {
  def eval(element: T): (Int, T) = {
    val blockID = element.hashCode() % numBlocks

    val blockIDResult = if(blockID < 0){
      blockID + numBlocks
    } else {
      blockID
    }

    (blockIDResult, element)
  }

  def getTypeInfo[K: TypeInformation]: TypeInformation[K] = implicitly[TypeInformation[K]]

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
    getTypeInfo[(Int, T)]
  }
}

class BlockGenerator[T] extends AggregateFunction[Block[T], tuple.Tuple2[Int, ArrayBuffer[T]]] {
  /**
    * Creates and init the Accumulator for this [[AggregateFunction]].
    *
    * @return the accumulator with the initial value
    */
  override def createAccumulator(): tuple.Tuple2[Int, ArrayBuffer[T]] =
    new tuple.Tuple2(0, ArrayBuffer[T]())

  def accumulate(acc: tuple.Tuple2[Int, ArrayBuffer[T]], id: Int, element: T): Unit = {
    acc.f0 = id
    acc.f1.append(element)
  }

  /**
    * Called every time when an aggregation result should be materialized.
    * The returned value could be either an early and incomplete result
    * (periodically emitted as data arrive) or the final result of the
    * aggregation.
    *
    * @param accumulator the accumulator which contains the current
    *                    aggregated results
    * @return the aggregation result
    */
  override def getValue(accumulator: tuple.Tuple2[Int, ArrayBuffer[T]]): Block[T] = {
    Block[T](accumulator.f0, accumulator.f1.toVector)
  }
}
