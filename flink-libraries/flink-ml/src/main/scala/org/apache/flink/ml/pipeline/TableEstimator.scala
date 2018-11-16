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

package org.apache.flink.ml.pipeline

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.flink.ml.common.{ParameterMap, WithParameters}
import org.apache.flink.table.api.Table

trait TableEstimator[Self] extends WithParameters {
  that: Self =>

  def fit[T](training: Table,fitParameters: ParameterMap = ParameterMap.Empty)
         (implicit fitOperation: TableFitOperation[Self, T]): Unit = {
    fitOperation.fit(this, fitParameters, training)
  }
}

object TableEstimator {
  implicit def fallbackFitOperation[
  Self: TypeTag, T: TypeTag]
  : TableFitOperation[Self, T] = {
    new TableFitOperation[Self, T]{
      override def fit(
          instance: Self,
          fitParameters: ParameterMap,
          input: Table)
      : Unit = {
        val self = typeOf[Self]

        throw new RuntimeException("There is no FitOperation defined for " + self +
          " which trains on a Table")
      }
    }
  }
}

trait TableFitOperation[Self, T] {
  def fit(instance: Self, fitParameters: ParameterMap,  input: Table): Unit
}
