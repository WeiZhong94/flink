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

package org.apache.flink.table.plan.nodes.dataset

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.{BatchQueryConfig, BatchTableEnvironment}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.plan.logical.LogicalWindow
import org.apache.flink.table.plan.nodes.CommonAggregate
import org.apache.flink.table.runtime.aggregate.AggregateUtil.CalcitePair
import org.apache.flink.types.Row

class DataSetWindowTableAggregate(
    val aggCall: AggregateCall,
    val groupSet: ImmutableBitSet,
    val window: LogicalWindow,
    val propertyExpressions: Seq[NamedWindowProperty],
    val rowRelDataType: RelDataType,
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode)
  extends DataSetWindowAggregate(
    window,
    propertyExpressions,
    cluster,
    traitSet,
    child,
    Seq(new CalcitePair(aggCall, "tableAggCall")),
    rowRelDataType,
    child.getRowType,
    groupSet.toArray
  )
    with CommonAggregate
    with DataSetRel {

  override def deriveRowType(): RelDataType = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    assert(inputs.size == 1)
    new DataSetWindowTableAggregate(
      aggCall,
      groupSet,
      window,
      propertyExpressions,
      rowRelDataType,
      cluster,
      traitSet,
      inputs.get(0))
  }

  override def toString: String = {
    s"TableAggregate(${
      if (!groupSet.isEmpty) {
        s"groupBy: (${groupingToString(child.getRowType, groupSet.toArray)}), "
      } else {
        ""
      }
    }window: ($window), " +
      s"select: (${
        aggregationToString(
          child.getRowType,
          groupSet.toArray,
          getRowType,
          Seq(new CalcitePair(aggCall, "tableAggCall")),
          propertyExpressions)
      }))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", groupingToString(child.getRowType, groupSet.toArray),
        !groupSet.isEmpty)
      .item("window", window)
      .item(
        "select", aggregationToString(
          child.getRowType,
          groupSet.toArray,
          getRowType,
          Seq(new CalcitePair(aggCall, "tableAggCall")),
          propertyExpressions))
  }


  /**
    * Translates the [[DataSetRel]] node into a [[DataSet]] operator.
    *
    * @param tableEnv    The [[BatchTableEnvironment]] of the translated Table.
    * @param queryConfig The configuration for the query to generate.
    * @return DataSet of type [[Row]]
    */
  override def translateToPlan(
      tableEnv: BatchTableEnvironment,
      queryConfig: BatchQueryConfig): DataSet[Row] = {
    super.translateToPlan(tableEnv, queryConfig)
  }

}
