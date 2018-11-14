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

import org.apache.flink.ml._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.ml.common.{ParameterMap, WithParameters}
import org.apache.flink.table.api.Table

trait TablePredictor[Self] extends TableEstimator[Self] with WithParameters {
  that: Self =>

  def predict[Testing, Prediction](
      testing: Table, predictParameters: ParameterMap = ParameterMap.Empty)
      (implicit predictor: TablePredictTableOperation[Self, Testing, Prediction]): Table = {
    predictor.predictTable(this, predictParameters, testing)
  }

  def evaluate[Testing, PredictionValue](
      testing: Table, evaluateParameters: ParameterMap = ParameterMap.Empty)
      (implicit evaluator: TableEvaluateTableOperation[Self, Testing, PredictionValue]): Table = {
    evaluator.evaluateTable(this, evaluateParameters, testing)
  }
}

object TablePredictor {
  implicit def defaultPredictTableOperation[
      Instance <: TableEstimator[Instance],
      Model,
      Testing,
      PredictionValue](
      implicit predictOperation: TablePredictOperation[Instance, Model, Testing, PredictionValue],
      testingTypeInformation: TypeInformation[Testing],
      predictionValueTypeInformation: TypeInformation[PredictionValue])
   : TablePredictTableOperation[Instance, Testing, (Testing, PredictionValue)] = {
    new TablePredictTableOperation[Instance, Testing, (Testing, PredictionValue)] {
      override def predictTable(instance: Instance,
                                predictParameters: ParameterMap,
                                input: Table): Table = {
        val resultingParameters = instance.parameters ++ predictParameters

        val model = predictOperation.getModel(instance, resultingParameters)

        implicit val resultTypeInformation = createTypeInformation[(Testing, PredictionValue)]

        input.mapWithBcVariable[Testing, Model, (Testing, PredictionValue)](model){
          (element, model) => {
            (element, predictOperation.predict(element, model))
          }
        }
      }
    }
  }

  implicit def defaultEvaluateTableOperation[
      Instance <: TableEstimator[Instance],
      Model,
      Testing,
      PredictionValue](
      implicit predictOperation: TablePredictOperation[Instance, Model, Testing, PredictionValue],
      testingTypeInformation: TypeInformation[Testing],
      predictionValueTypeInformation: TypeInformation[PredictionValue])
    : TableEvaluateTableOperation[Instance, (Testing, PredictionValue), PredictionValue] = {
    new TableEvaluateTableOperation[Instance, (Testing, PredictionValue), PredictionValue] {
      override def evaluateTable(
          instance: Instance,
          evaluateParameters: ParameterMap,
          testing: Table): Table = {
        val resultingParameters = instance.parameters ++ evaluateParameters
        val model = predictOperation.getModel(instance, resultingParameters)

        implicit val resultTypeInformation = createTypeInformation[(Testing, PredictionValue)]

        testing.mapWithBcVariable[
          (Testing, PredictionValue),
          Model,
          (PredictionValue, PredictionValue)](model){
          (element, model) => {
            (element._2, predictOperation.predict(element._1, model))
          }
        }
      }
    }
  }
}

trait TablePredictTableOperation[Self, Testing, Prediction] extends Serializable {
  def predictTable(instance: Self,
                   predictParameters: ParameterMap,
                   input: Table): Table
}

trait TablePredictOperation[Instance, Model, Testing, Prediction] extends Serializable {
  def getModel(instance: Instance, predictParameters: ParameterMap): Table

  def predict(value: Testing, model: Model):
  Prediction
}

trait TableEvaluateTableOperation[Instance, Testing, Prediction] extends Serializable {
  def evaluateTable(
    instance: Instance, evaluateParameters: ParameterMap, testing: Table): Table
}
