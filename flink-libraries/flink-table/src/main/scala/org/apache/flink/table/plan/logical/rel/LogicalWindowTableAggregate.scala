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

package org.apache.flink.table.plan.logical.rel

import java.util

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.utils.TableAggSqlFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.getFieldInfo
import org.apache.flink.table.plan.logical.LogicalWindow

class LogicalWindowTableAggregate(
    val aggCall: AggregateCall,
    val groupSet: ImmutableBitSet,
    val window: LogicalWindow,
    val propertyExpressions: Seq[NamedWindowProperty],
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode) extends SingleRel(cluster, traitSet, child) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    assert(inputs.size == 1)
    new LogicalWindowTableAggregate(
      aggCall,
      groupSet,
      window,
      propertyExpressions,
      cluster,
      traitSet,
      inputs.get(0))
  }

  override def deriveRowType(): RelDataType = {
    val func = aggCall.getAggregation.asInstanceOf[TableAggSqlFunction]
    val typeFactory = getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val builder = typeFactory.builder
    val inputRowType = getInput.getRowType
    val fieldList = inputRowType.getFieldList
    val groupList = groupSet.toArray

    //first part is groupKey
    groupList.foreach(groupKey => {
      val field = fieldList.get(groupKey)
      builder.add(field)
    })

    //second part is function result
    val (generatedNames, fieldIndexes, fieldTypes) = getFieldInfo(func.tableReturnType)
    val relDataTypes = fieldTypes.map(typeFactory.createTypeFromTypeInfo(_, isNullable = true))
    generatedNames.zip(relDataTypes).foreach(t => builder.add(t._1, t._2))

    //third part is window property
    propertyExpressions.foreach { namedProp =>
      builder.add(
        namedProp.name,
        typeFactory.createTypeFromTypeInfo(namedProp.property.resultType, isNullable = false)
      )
    }

    builder.build()
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("group", groupSet)
      .item("tableAgg", aggCall)
    for (property <- propertyExpressions) {
      pw.item(property.name, property.property)
    }
    pw
  }
}

object LogicalWindowTableAggregate {

  def create(
      window: LogicalWindow,
      namedProperties: Seq[NamedWindowProperty],
      aggregate: Aggregate): LogicalWindowTableAggregate = {
    val cluster: RelOptCluster = aggregate.getCluster
    val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
    new LogicalWindowTableAggregate(
      aggregate.getAggCallList.get(0),
      aggregate.getGroupSet,
      window,
      namedProperties,
      cluster,
      traitSet,
      aggregate.getInput)
  }
}