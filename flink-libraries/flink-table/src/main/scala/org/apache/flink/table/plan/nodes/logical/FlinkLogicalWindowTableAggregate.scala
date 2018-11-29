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

package org.apache.flink.table.plan.nodes.logical

import java.util

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelShuttle, SingleRel}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.plan.logical.LogicalWindow
import org.apache.flink.table.plan.logical.rel.LogicalWindowTableAggregate
import org.apache.flink.table.plan.nodes.FlinkConventions

class FlinkLogicalWindowTableAggregate(
    val aggCall: AggregateCall,
    val groupSet: ImmutableBitSet,
    val window: LogicalWindow,
    val propertyExpressions: Seq[NamedWindowProperty],
    val rowRelDataType: RelDataType,
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode)
  extends SingleRel(cluster, traitSet, child)
    with FlinkLogicalRel {

  def getWindow: LogicalWindow = window

  def getNamedProperties: Seq[NamedWindowProperty] = propertyExpressions

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    assert(inputs.size == 1)
    new FlinkLogicalWindowTableAggregate(
      aggCall,
      groupSet,
      window,
      propertyExpressions,
      rowRelDataType,
      cluster,
      traitSet,
      inputs.get(0))
  }

  override def deriveRowType(): RelDataType = rowRelDataType

  override def accept(shuttle: RelShuttle): RelNode = shuttle.visit(this)

  //TODO: override this
  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost =
    super.computeSelfCost(planner, mq)

  //TODO: override this
  override def estimateRowCount(mq: RelMetadataQuery): Double = super.estimateRowCount(mq)
}

class FlinkLogicalWindowTableAggregateConverter
  extends ConverterRule(
    classOf[LogicalWindowTableAggregate],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalWindowTableAggregateConverter") {

  override def convert(rel: RelNode): RelNode = {
    val agg = rel.asInstanceOf[LogicalWindowTableAggregate]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val newInput = RelOptRule.convert(agg.getInput, FlinkConventions.LOGICAL)

    new FlinkLogicalWindowTableAggregate(
      agg.aggCall,
      agg.groupSet,
      agg.window,
      agg.propertyExpressions,
      agg.deriveRowType(),
      rel.getCluster,
      traitSet,
      newInput)
  }
}

object FlinkLogicalWindowTableAggregate {
  val CONVERTER = new FlinkLogicalWindowTableAggregateConverter
}
