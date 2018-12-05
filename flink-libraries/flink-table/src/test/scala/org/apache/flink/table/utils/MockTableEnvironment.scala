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

package org.apache.flink.table.utils

import _root_.java.lang.{Boolean => JBool}

import org.apache.calcite.tools.RuleSet
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.scala.{DataSet => ScalaDataSet}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.{DataStream => ScalaDataStream}
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{BatchTableDescriptor, ConnectorDescriptor, StreamTableDescriptor, TableDescriptor}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource

class MockTableEnvironment extends AbstractTableEnvironment(new TableConfig) {

  override private[flink] def writeToSink[T](
      table: Table,
      sink: TableSink[T],
      queryConfig: QueryConfig): Unit = ???

  override protected def checkValidTableName(name: String): Unit = ???

  override def sqlQuery(query: String): Table = ???

  override protected def getBuiltInNormRuleSet: RuleSet = ???

  override protected def getBuiltInPhysicalOptRuleSet: RuleSet = ???

  override def registerTableSink(
      name: String,
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]],
      tableSink: TableSink[_]): Unit = ???

  override def registerTableSink(name: String, tableSink: TableSink[_]): Unit = ???

  override protected def createUniqueTableName(): String = ???

  override protected def registerTableSourceInternal(name: String, tableSource: TableSource[_])
    : Unit = ???

  override def explain(table: Table): String = ???

  override def connect(connectorDescriptor: ConnectorDescriptor): TableDescriptor = ???

  override def connectForStream(
      connectorDescriptor: ConnectorDescriptor): StreamTableDescriptor = ???

  override def connectForBatch(
      connectorDescriptor: ConnectorDescriptor): BatchTableDescriptor = ???

  override def fromDataSet[T](dataSet: ScalaDataSet[T]): Table = ???

  override def fromDataSet[T](dataSet: ScalaDataSet[T], fields: Expression*): Table = ???

  override def registerDataSet[T](name: String, dataSet: ScalaDataSet[T]): Unit = ???

  override def registerDataSet[T](
      name: String,
      dataSet: ScalaDataSet[T], fields: Expression*): Unit = ???

  override def toDataSetScala[T: TypeInformation](table: Table): ScalaDataSet[T] = ???

  override def toDataSetScala[T: TypeInformation](
      table: Table,
      queryConfig: BatchQueryConfig): ScalaDataSet[T] = ???

  override def registerFunctionScala[T: TypeInformation](
      name: String,
      tf: TableFunction[T]): Unit = ???

  override def registerFunctionScala[T: TypeInformation, ACC: TypeInformation](
      name: String,
      f: AggregateFunction[T, ACC]): Unit = ???

  override def fromDataSet[T](dataSet: DataSet[T]): Table = ???

  override def fromDataSet[T](dataSet: DataSet[T], fields: String): Table = ???

  override def registerDataSet[T](name: String, dataSet: DataSet[T]): Unit = ???

  override def registerDataSet[T](name: String, dataSet: DataSet[T], fields: String): Unit = ???

  override def toDataSet[T](table: Table, clazz: Class[T]): DataSet[T] = ???

  override def toDataSet[T](table: Table, typeInfo: TypeInformation[T]): DataSet[T] = ???

  override def toDataSet[T](
      table: Table,
      clazz: Class[T],
      queryConfig: BatchQueryConfig): DataSet[T] = ???

  override def toDataSet[T](
      table: Table,
      typeInfo: TypeInformation[T],
      queryConfig: BatchQueryConfig): DataSet[T] = ???

  override def registerFunction[T](name: String, tf: TableFunction[T]): Unit = ???

  override def registerFunction[T, ACC](name: String, f: AggregateFunction[T, ACC]): Unit = ???

  override def fromDataStream[T](dataStream: ScalaDataStream[T]): Table = ???

  override def fromDataStream[T](dataStream: ScalaDataStream[T], fields: Expression*): Table = ???

  override def registerDataStream[T](name: String, dataStream: ScalaDataStream[T]): Unit = ???

  override def registerDataStream[T](
      name: String,
      dataStream: ScalaDataStream[T],
      fields: Expression*): Unit = ???

  override def toAppendStreamScala[T: TypeInformation](table: Table): ScalaDataStream[T] = ???

  override def toAppendStreamScala[T: TypeInformation](
      table: Table,
      queryConfig: StreamQueryConfig): ScalaDataStream[T] = ???

  override def toRetractStreamScala[T: TypeInformation](
      table: Table): ScalaDataStream[(Boolean, T)] = ???

  override def toRetractStreamScala[T: TypeInformation](
      table: Table,
      queryConfig: StreamQueryConfig): ScalaDataStream[(Boolean, T)] = ???

  override def fromDataStream[T](dataStream: DataStream[T]): Table = ???

  override def fromDataStream[T](dataStream: DataStream[T], fields: String): Table = ???

  override def registerDataStream[T](name: String, dataStream: DataStream[T]): Unit = ???

  override def registerDataStream[T](
      name: String,
      dataStream: DataStream[T],
      fields: String): Unit = ???

  override def toAppendStream[T](table: Table, clazz: Class[T]): DataStream[T] = ???

  override def toAppendStream[T](table: Table, typeInfo: TypeInformation[T]): DataStream[T] = ???

  override def toAppendStream[T](
      table: Table,
      clazz: Class[T],
      queryConfig: StreamQueryConfig): DataStream[T] = ???

  override def toAppendStream[T](
      table: Table,
      typeInfo: TypeInformation[T],
      queryConfig: StreamQueryConfig): DataStream[T] = ???

  override def toRetractStream[T](
      table: Table,
      clazz: Class[T]): DataStream[JTuple2[JBool, T]] = ???

  override def toRetractStream[T](
      table: Table,
      typeInfo: TypeInformation[T]): DataStream[JTuple2[JBool, T]] = ???

  override def toRetractStream[T](
      table: Table,
      clazz: Class[T],
      queryConfig: StreamQueryConfig): DataStream[JTuple2[JBool, T]] = ???

  override def toRetractStream[T](
      table: Table,
      typeInfo: TypeInformation[T],
      queryConfig: StreamQueryConfig): DataStream[JTuple2[JBool, T]] = ???
}
