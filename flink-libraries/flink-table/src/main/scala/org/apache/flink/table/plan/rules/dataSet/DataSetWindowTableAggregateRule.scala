package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetWindowTableAggregate
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalWindowTableAggregate

class DataSetWindowTableAggregateRule
  extends ConverterRule(
    classOf[FlinkLogicalWindowTableAggregate],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetWindowTableAggregateRule") {

  override def convert(rel: RelNode): RelNode = {
    val agg: FlinkLogicalWindowTableAggregate = rel.asInstanceOf[FlinkLogicalWindowTableAggregate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    val convInput: RelNode = RelOptRule.convert(agg.getInput, FlinkConventions.DATASET)

    new DataSetWindowTableAggregate(
      agg.aggCall,
      agg.groupSet,
      agg.window,
      agg.propertyExpressions,
      agg.rowRelDataType,
      rel.getCluster,
      traitSet,
      convInput
    )
  }

}

object DataSetWindowTableAggregateRule {
  val INSTANCE: RelOptRule = new DataSetWindowTableAggregateRule
}
