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
package org.apache.flink.table.api.java

import _root_.java.lang.{Boolean => JBool}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.api._
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}
import org.apache.flink.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.{DataStream => ScalaDataStream}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * The [[TableEnvironment]] for a Java [[StreamExecutionEnvironment]].
  *
  * A TableEnvironment can be used to:
  * - convert a [[DataStream]] to a [[Table]]
  * - register a [[DataStream]] in the [[TableEnvironment]]'s catalog
  * - register a [[Table]] in the [[TableEnvironment]]'s catalog
  * - scan a registered table to obtain a [[Table]]
  * - specify a SQL query on registered tables to obtain a [[Table]]
  * - convert a [[Table]] into a [[DataStream]]
  * - explain the AST and execution plan of a [[Table]]
  *
  * @param execEnv The Java [[StreamExecutionEnvironment]] of the TableEnvironment.
  * @param config The configuration of the TableEnvironment.
  */
class StreamTableEnvironment(
    execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends org.apache.flink.table.api.StreamTableEnvironment(execEnv, config) {

  /**
    * Converts the given [[DataStream]] into a [[Table]].
    *
    * The field names of the [[Table]] are automatically derived from the type of the
    * [[DataStream]].
    *
    * @param dataStream The [[DataStream]] to be converted.
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromDataStream[T](dataStream: DataStream[T]): Table = {

    val name = createUniqueTableName()
    registerDataStreamInternal(name, dataStream)
    scan(name)
  }

  /**
    * Converts the given [[DataStream]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   DataStream<Tuple2<String, Long>> stream = ...
    *   Table tab = tableEnv.fromDataStream(stream, "a, b")
    * }}}
    *
    * @param dataStream The [[DataStream]] to be converted.
    * @param fields The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromDataStream[T](dataStream: DataStream[T], fields: String): Table = {
    val exprs = ExpressionParser
      .parseExpressionList(fields)
      .toArray

    val name = createUniqueTableName()
    registerDataStreamInternal(name, dataStream, exprs)
    scan(name)
  }

  /**
    * Registers the given [[DataStream]] as table in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived
    * from the type of the [[DataStream]].
    *
    * @param name The name under which the [[DataStream]] is registered in the catalog.
    * @param dataStream The [[DataStream]] to register.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerDataStream[T](name: String, dataStream: DataStream[T]): Unit = {

    checkValidTableName(name)
    registerDataStreamInternal(name, dataStream)
  }

  /**
    * Registers the given [[DataStream]] as table with specified field names in the
    * [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   DataStream<Tuple2<String, Long>> set = ...
    *   tableEnv.registerDataStream("myTable", set, "a, b")
    * }}}
    *
    * @param name The name under which the [[DataStream]] is registered in the catalog.
    * @param dataStream The [[DataStream]] to register.
    * @param fields The field names of the registered table.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerDataStream[T](name: String, dataStream: DataStream[T], fields: String): Unit = {
    val exprs = ExpressionParser
      .parseExpressionList(fields)
      .toArray

    checkValidTableName(name)
    registerDataStreamInternal(name, dataStream, exprs)
  }

  /**
    * Converts the given [[Table]] into an append [[DataStream]] of a specified type.
    *
    * The [[Table]] must only have insert (append) changes. If the [[Table]] is also modified
    * by update or delete changes, the conversion will fail.
    *
    * The fields of the [[Table]] are mapped to [[DataStream]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @param clazz The class of the type of the resulting [[DataStream]].
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toAppendStream[T](table: Table, clazz: Class[T]): DataStream[T] = {
    toAppendStream(table, clazz, queryConfig)
  }

  /**
    * Converts the given [[Table]] into an append [[DataStream]] of a specified type.
    *
    * The [[Table]] must only have insert (append) changes. If the [[Table]] is also modified
    * by update or delete changes, the conversion will fail.
    *
    * The fields of the [[Table]] are mapped to [[DataStream]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @param typeInfo The [[TypeInformation]] that specifies the type of the [[DataStream]].
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toAppendStream[T](table: Table, typeInfo: TypeInformation[T]): DataStream[T] = {
    toAppendStream(table, typeInfo, queryConfig)
  }

  /**
    * Converts the given [[Table]] into an append [[DataStream]] of a specified type.
    *
    * The [[Table]] must only have insert (append) changes. If the [[Table]] is also modified
    * by update or delete changes, the conversion will fail.
    *
    * The fields of the [[Table]] are mapped to [[DataStream]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @param clazz The class of the type of the resulting [[DataStream]].
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toAppendStream[T](
      table: Table,
      clazz: Class[T],
      queryConfig: StreamQueryConfig): DataStream[T] = {
    val typeInfo = TypeExtractor.createTypeInfo(clazz)
    TableEnvironment.validateType(typeInfo)
    translate[T](table, queryConfig, updatesAsRetraction = false, withChangeFlag = false)(typeInfo)
  }

  /**
    * Converts the given [[Table]] into an append [[DataStream]] of a specified type.
    *
    * The [[Table]] must only have insert (append) changes. If the [[Table]] is also modified
    * by update or delete changes, the conversion will fail.
    *
    * The fields of the [[Table]] are mapped to [[DataStream]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @param typeInfo The [[TypeInformation]] that specifies the type of the [[DataStream]].
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toAppendStream[T](
      table: Table,
      typeInfo: TypeInformation[T],
      queryConfig: StreamQueryConfig): DataStream[T] = {
    TableEnvironment.validateType(typeInfo)
    translate[T](table, queryConfig, updatesAsRetraction = false, withChangeFlag = false)(typeInfo)
  }

  /**
    * Converts the given [[Table]] into a [[DataStream]] of add and retract messages.
    * The message will be encoded as [[JTuple2]]. The first field is a [[JBool]] flag,
    * the second field holds the record of the specified type [[T]].
    *
    * A true [[JBool]] flag indicates an add message, a false flag indicates a retract message.
    *
    * The fields of the [[Table]] are mapped to the requested type as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @param clazz The class of the requested record type.
    * @tparam T The type of the requested record type.
    * @return The converted [[DataStream]].
    */
  def toRetractStream[T](
      table: Table,
      clazz: Class[T]): DataStream[JTuple2[JBool, T]] = {

    toRetractStream(table, clazz, queryConfig)
  }

  /**
    * Converts the given [[Table]] into a [[DataStream]] of add and retract messages.
    * The message will be encoded as [[JTuple2]]. The first field is a [[JBool]] flag,
    * the second field holds the record of the specified type [[T]].
    *
    * A true [[JBool]] flag indicates an add message, a false flag indicates a retract message.
    *
    * The fields of the [[Table]] are mapped to the requested type as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @param typeInfo The [[TypeInformation]] of the requested record type.
    * @tparam T The type of the requested record type.
    * @return The converted [[DataStream]].
    */
  def toRetractStream[T](
      table: Table,
      typeInfo: TypeInformation[T]): DataStream[JTuple2[JBool, T]] = {

    toRetractStream(table, typeInfo, queryConfig)
  }

  /**
    * Converts the given [[Table]] into a [[DataStream]] of add and retract messages.
    * The message will be encoded as [[JTuple2]]. The first field is a [[JBool]] flag,
    * the second field holds the record of the specified type [[T]].
    *
    * A true [[JBool]] flag indicates an add message, a false flag indicates a retract message.
    *
    * The fields of the [[Table]] are mapped to the requested type as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @param clazz The class of the requested record type.
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the requested record type.
    * @return The converted [[DataStream]].
    */
  def toRetractStream[T](
      table: Table,
      clazz: Class[T],
      queryConfig: StreamQueryConfig): DataStream[JTuple2[JBool, T]] = {

    val typeInfo = TypeExtractor.createTypeInfo(clazz)
    TableEnvironment.validateType(typeInfo)
    val resultType = new TupleTypeInfo[JTuple2[JBool, T]](Types.BOOLEAN, typeInfo)
    translate[JTuple2[JBool, T]](
      table,
      queryConfig,
      updatesAsRetraction = true,
      withChangeFlag = true)(resultType)
  }

  /**
    * Converts the given [[Table]] into a [[DataStream]] of add and retract messages.
    * The message will be encoded as [[JTuple2]]. The first field is a [[JBool]] flag,
    * the second field holds the record of the specified type [[T]].
    *
    * A true [[JBool]] flag indicates an add message, a false flag indicates a retract message.
    *
    * The fields of the [[Table]] are mapped to the requested type as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @param typeInfo The [[TypeInformation]] of the requested record type.
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the requested record type.
    * @return The converted [[DataStream]].
    */
  def toRetractStream[T](
      table: Table,
      typeInfo: TypeInformation[T],
      queryConfig: StreamQueryConfig): DataStream[JTuple2[JBool, T]] = {

    TableEnvironment.validateType(typeInfo)
    val resultTypeInfo = new TupleTypeInfo[JTuple2[JBool, T]](
      Types.BOOLEAN,
      typeInfo
    )
    translate[JTuple2[JBool, T]](
      table,
      queryConfig,
      updatesAsRetraction = true,
      withChangeFlag = true)(resultTypeInfo)
  }

  /**
    * Registers a [[TableFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param tf The TableFunction to register.
    */
  def registerFunction(name: String, tf: TableFunction[_]): Unit = {
    val typeInfo: TypeInformation[_] = TypeExtractor
      .createTypeInfo(tf, classOf[TableFunction[_]], tf.getClass, 0)
      .asInstanceOf[TypeInformation[_]]

    registerTableFunctionInternal(typeInfo, name, tf)
  }

  /**
    * Registers an [[AggregateFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param f The AggregateFunction to register.
    */
  def registerFunction(name: String, f: AggregateFunction[_, _])
  : Unit = {
    val typeInfo: TypeInformation[_] = TypeExtractor
      .createTypeInfo(f, classOf[AggregateFunction[_, _]], f.getClass, 0)
      .asInstanceOf[TypeInformation[_]]

    val accTypeInfo: TypeInformation[_] = TypeExtractor
      .createTypeInfo(f, classOf[AggregateFunction[_, _]], f.getClass, 1)
      .asInstanceOf[TypeInformation[_]]

    registerAggregateFunctionInternal(typeInfo, accTypeInfo, name, f)
  }

  protected def unsupportedScalaMethod =
    new UnsupportedOperationException("This method is not supported in Java environment!")

  override def registerFunctionScala[T: TypeInformation](
      name: String, tf: TableFunction[T]): Unit =
    throw unsupportedScalaMethod

  override def registerFunctionScala[T: TypeInformation, ACC: TypeInformation](
      name: String, f: AggregateFunction[T, ACC]): Unit =
    throw unsupportedScalaMethod

  override def fromDataStream[T](dataStream: ScalaDataStream[T]): Table =
    throw unsupportedScalaMethod

  override def fromDataStream[T](dataStream: ScalaDataStream[T], fields: Expression*): Table =
    throw unsupportedScalaMethod

  override def registerDataStream[T](name: String, dataStream: ScalaDataStream[T]): Unit =
    throw unsupportedScalaMethod

  override def registerDataStream[T](
      name: String, dataStream: ScalaDataStream[T], fields: Expression*): Unit =
    throw unsupportedScalaMethod

  override def toAppendStreamScala[T: TypeInformation](table: Table): ScalaDataStream[T] =
    throw unsupportedScalaMethod

  override def toAppendStreamScala[T: TypeInformation](
      table: Table, queryConfig: StreamQueryConfig): ScalaDataStream[T] =
    throw unsupportedScalaMethod

  override def toRetractStreamScala[T: TypeInformation](
      table: Table): ScalaDataStream[(Boolean, T)] =
    throw unsupportedScalaMethod

  override def toRetractStreamScala[T: TypeInformation](
      table: Table, queryConfig: StreamQueryConfig): ScalaDataStream[(Boolean, T)] =
    throw unsupportedScalaMethod
}
