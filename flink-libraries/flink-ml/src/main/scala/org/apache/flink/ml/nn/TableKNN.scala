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

package org.apache.flink.ml.nn

import org.apache.flink.api.common.operators.base.CrossOperatorBase.CrossHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.ml._
import org.apache.flink.ml.common.{Parameter, ParameterMap, TableFlinkMLTools}
import org.apache.flink.ml.metrics.distances.{DistanceMetric, EuclideanDistanceMetric, SquaredEuclideanDistanceMetric}
import org.apache.flink.ml.pipeline.{TableFitOperation, TablePredictTableOperation, TablePredictor}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.ml.math.{DenseVector, Vector => FlinkVector}

import scala.reflect.ClassTag
class TableKNN extends TablePredictor[TableKNN] {

  import TableKNN._

  var trainingSet: Option[Table] = None

  /** Sets K
    *
    * @param k the number of selected points as neighbors
    */
  def setK(k: Int): TableKNN = {
    require(k > 0, "K must be positive.")
    parameters.add(K, k)
    this
  }

  /** Sets the distance metric
    *
    * @param metric the distance metric to calculate distance between two points
    */
  def setDistanceMetric(metric: DistanceMetric): TableKNN = {
    parameters.add(DistanceMetric, metric)
    this
  }

  /** Sets the number of data blocks/partitions
    *
    * @param n the number of data blocks
    */
  def setBlocks(n: Int): TableKNN = {
    require(n > 0, "Number of blocks must be positive.")
    parameters.add(Blocks, n)
    this
  }

  /** Sets the Boolean variable that decides whether to use the QuadTree or not */
  def setUseQuadTree(useQuadTree: Boolean): TableKNN = {
    if (useQuadTree) {
      require(parameters(DistanceMetric).isInstanceOf[SquaredEuclideanDistanceMetric] ||
        parameters(DistanceMetric).isInstanceOf[EuclideanDistanceMetric])
    }
    parameters.add(UseQuadTree, useQuadTree)
    this
  }

  /** Parameter a user can specify if one of the training or test sets are small
    *
    * @param sizeHint cross hint tells the system which sizes to expect from the data sets
    */
  def setSizeHint(sizeHint: CrossHint): TableKNN = {
    parameters.add(SizeHint, sizeHint)
    this
  }
}

object TableKNN {

  case object K extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(5)
  }

  case object DistanceMetric extends Parameter[DistanceMetric] {
    val defaultValue: Option[DistanceMetric] = Some(EuclideanDistanceMetric())
  }

  case object Blocks extends Parameter[Int] {
    val defaultValue: Option[Int] = None
  }

  case object UseQuadTree extends Parameter[Boolean] {
    val defaultValue: Option[Boolean] = None
  }

  case object SizeHint extends Parameter[CrossHint] {
    val defaultValue: Option[CrossHint] = None
  }

  def apply(): TableKNN = {
    new TableKNN
  }

  implicit def fitKNN[T <: FlinkVector : TypeInformation]: TableFitOperation[TableKNN] = {
    new TableFitOperation[TableKNN] {
      override def fit(instance: TableKNN, fitParameters: ParameterMap, input: Table): Unit = {
        val resultParameters = instance.parameters ++ fitParameters

        require(resultParameters.get(K).isDefined, "K is needed for calculation")
        require(resultParameters.get(Blocks).isDefined, "Blocks is needed for calculation")

        val blocks = resultParameters.get(Blocks).get
        val partitioner = TableFlinkMLTools.ModuloKeyPartitionFunction
        val inputAsVector = input

        instance.trainingSet = Some(TableFlinkMLTools.block(inputAsVector, blocks, Some(partitioner)))
      }
    }
  }

  implicit def predictValues[T <: FlinkVector : ClassTag : TypeInformation] = {
    new TablePredictTableOperation[TableKNN, T, (FlinkVector, Array[FlinkVector])] {
      override def predictTable(
          instance: TableKNN,
          predictParameters: ParameterMap,
          input: Table): Table = {
        val resultParameters = instance.parameters ++ predictParameters
        require(resultParameters.get(K).isDefined, "K is needed for calculation")
        require(resultParameters.get(Blocks).isDefined, "Blocks is needed for calculation")

        instance.trainingSet match {
          case Some(trainingSet) =>
            val k = resultParameters.get(K).get
            val blocks = resultParameters.get(Blocks).get
            val metric = resultParameters.get(DistanceMetric).get
            val partitioner = TableFlinkMLTools.ModuloKeyPartitionFunction

            val inputWithId = input.zipWithRandomLong()

            val inputSplit = TableFlinkMLTools.block(inputWithId, blocks, Some(partitioner))

            val crossTuned = trainingSet.as('train).join(inputSplit.as('test))


            null
      }
    }
  }
}
