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
package org.apache.flink.table.api

import _root_.java.lang.{Boolean => JBool}
import _root_.java.lang.reflect.Modifier

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.{ExecutionEnvironment => JavaBatchExecEnv}
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.scala.{DataSet => ScalaDataSet, ExecutionEnvironment => ScalaBatchExecEnv}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaStreamExecEnv}
import org.apache.flink.streaming.api.scala.{DataStream => ScalaDataStream, StreamExecutionEnvironment => ScalaStreamExecEnv}
import org.apache.flink.table.catalog.ExternalCatalog
import org.apache.flink.table.descriptors.{BatchTableDescriptor, ConnectorDescriptor, StreamTableDescriptor, TableDescriptor}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource

import _root_.scala.annotation.varargs

abstract class TableEnvironment {

  /**
    * Creates a table from a table source.
    *
    * @param source table source used as table
    */
  def fromTableSource(source: TableSource[_]): Table

  /**
    * Registers an [[ExternalCatalog]] under a unique name in the TableEnvironment's schema.
    * All tables registered in the [[ExternalCatalog]] can be accessed.
    *
    * @param name            The name under which the externalCatalog will be registered
    * @param externalCatalog The externalCatalog to register
    */
  def registerExternalCatalog(name: String, externalCatalog: ExternalCatalog): Unit

  /**
    * Gets a registered [[ExternalCatalog]] by name.
    *
    * @param name The name to look up the [[ExternalCatalog]]
    * @return The [[ExternalCatalog]]
    */
  def getRegisteredExternalCatalog(name: String): ExternalCatalog

  /**
    * Registers a [[ScalarFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  def registerFunction(name: String, function: ScalarFunction): Unit

  /**
    * Registers a [[Table]] under a unique name in the TableEnvironment's catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name  The name under which the table will be registered.
    * @param table The table to register.
    */
  def registerTable(name: String, table: Table): Unit

  /**
    * Registers an external [[TableSource]] in this [[AbstractTableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  def registerTableSource(name: String, tableSource: TableSource[_]): Unit

  /**
    * Registers an external [[TableSink]] with given field names and types in this
    * [[AbstractTableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name       The name under which the [[TableSink]] is registered.
    * @param fieldNames The field names to register with the [[TableSink]].
    * @param fieldTypes The field types to register with the [[TableSink]].
    * @param tableSink  The [[TableSink]] to register.
    */
  def registerTableSink(
      name: String,
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]],
      tableSink: TableSink[_]): Unit

  /**
    * Registers an external [[TableSink]] with already configured field names and field types in
    * this [[AbstractTableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name           The name under which the [[TableSink]] is registered.
    * @param configuredSink The configured [[TableSink]] to register.
    */
  def registerTableSink(name: String, configuredSink: TableSink[_]): Unit

  /**
    * Scans a registered table and returns the resulting [[Table]].
    *
    * A table to scan must be registered in the TableEnvironment. It can be either directly
    * registered as DataStream, DataSet, or Table or as member of an [[ExternalCatalog]].
    *
    * Examples:
    *
    * - Scanning a directly registered table
    * {{{
    *   val tab: Table = tableEnv.scan("tableName")
    * }}}
    *
    * - Scanning a table from a registered catalog
    * {{{
    *   val tab: Table = tableEnv.scan("catalogName", "dbName", "tableName")
    * }}}
    *
    * @param tablePath The path of the table to scan.
    * @throws TableException if no table is found using the given table path.
    * @return The resulting [[Table]].
    */
  @throws[TableException]
  @varargs
  def scan(tablePath: String*): Table

  /**
    * Creates a table source and/or table sink from a descriptor.
    *
    * Descriptors allow for declaring the communication to external systems in an
    * implementation-agnostic way. The classpath is scanned for suitable table factories that match
    * the desired configuration.
    *
    * The following example shows how to read from a connector using a JSON format and
    * registering a table source as "MyTable":
    *
    * {{{
    *
    * tableEnv
    *   .connect(
    *     new ExternalSystemXYZ()
    *       .version("0.11"))
    *   .withFormat(
    *     new Json()
    *       .jsonSchema("{...}")
    *       .failOnMissingField(false))
    *   .withSchema(
    *     new Schema()
    *       .field("user-name", "VARCHAR").from("u_name")
    *       .field("count", "DECIMAL")
    *   .registerSource("MyTable")
    * }}}
    *
    * @param connectorDescriptor connector descriptor describing the external system
    */
  def connect(connectorDescriptor: ConnectorDescriptor): TableDescriptor

  def connectForStream(connectorDescriptor: ConnectorDescriptor): StreamTableDescriptor

  def connectForBatch(connectorDescriptor: ConnectorDescriptor): BatchTableDescriptor

  /**
    * Gets the names of all tables registered in this environment.
    *
    * @return A list of the names of all registered tables.
    */
  def listTables(): Array[String]

  /**
    * Gets the names of all functions registered in this environment.
    */
  def listUserDefinedFunctions(): Array[String]

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  def explain(table: Table): String

  /**
    * Returns completion hints for the given statement at the given cursor position.
    * The completion happens case insensitively.
    *
    * @param statement Partial or slightly incorrect SQL statement
    * @param position  cursor position
    * @return completion hints that fit at the current cursor position
    */
  def getCompletionHints(statement: String, position: Int): Array[String]

  /**
    * Evaluates a SQL query on registered tables and retrieves the result as a [[Table]].
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
    *
    * {{{
    *   val table: Table = ...
    *   // the table is not registered to the table environment
    *   tEnv.sqlQuery(s"SELECT * FROM $table")
    * }}}
    *
    * @param query The SQL query to evaluate.
    * @return The result of the query as Table
    */
  def sqlQuery(query: String): Table

  /**
    * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
    * NOTE: Currently only SQL INSERT statements are supported.
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
    *
    * {{{
    *   // register the table sink into which the result is inserted.
    *   tEnv.registerTableSink("sinkTable", fieldNames, fieldsTypes, tableSink)
    *   val sourceTable: Table = ...
    *   // sourceTable is not registered to the table environment
    *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM $sourceTable")
    * }}}
    *
    * @param stmt The SQL statement to evaluate.
    */
  def sqlUpdate(stmt: String): Unit

  /**
    * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
    * NOTE: Currently only SQL INSERT statements are supported.
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
    *
    * {{{
    *   // register the table sink into which the result is inserted.
    *   tEnv.registerTableSink("sinkTable", fieldNames, fieldsTypes, tableSink)
    *   val sourceTable: Table = ...
    *   // sourceTable is not registered to the table environment
    *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM $sourceTable")
    * }}}
    *
    * @param stmt   The SQL statement to evaluate.
    * @param config The [[QueryConfig]] to use.
    */
  def sqlUpdate(stmt: String, config: QueryConfig): Unit

  /**
    * Converts the given [[DataSet]] into a [[Table]].
    *
    * The field names of the [[Table]] are automatically derived from the type of the [[DataSet]].
    *
    * @param dataSet The [[DataSet]] to be converted.
    * @tparam T The type of the [[DataSet]].
    * @return The converted [[Table]].
    */
  def fromDataSet[T](dataSet: ScalaDataSet[T]): Table

  /**
    * Converts the given [[DataSet]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   val set: DataSet[(String, Long)] = ...
    *   val tab: Table = tableEnv.fromDataSet(set, 'a, 'b)
    * }}}
    *
    * @param dataSet The [[DataSet]] to be converted.
    * @param fields  The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataSet]].
    * @return The converted [[Table]].
    */
  def fromDataSet[T](dataSet: ScalaDataSet[T], fields: Expression*): Table

  /**
    * Registers the given [[DataSet]] as table in the
    * [[AbstractTableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived from the type of the [[DataSet]].
    *
    * @param name    The name under which the [[DataSet]] is registered in the catalog.
    * @param dataSet The [[DataSet]] to register.
    * @tparam T The type of the [[DataSet]] to register.
    */
  def registerDataSet[T](name: String, dataSet: ScalaDataSet[T]): Unit

  /**
    * Registers the given [[DataSet]] as table with specified field names in the
    * [[AbstractTableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   val set: DataSet[(String, Long)] = ...
    *   tableEnv.registerDataSet("myTable", set, 'a, 'b)
    * }}}
    *
    * @param name    The name under which the [[DataSet]] is registered in the catalog.
    * @param dataSet The [[DataSet]] to register.
    * @param fields  The field names of the registered table.
    * @tparam T The type of the [[DataSet]] to register.
    */
  def registerDataSet[T](name: String, dataSet: ScalaDataSet[T], fields: Expression*): Unit

  /**
    * Converts the given [[Table]] into a [[DataSet]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataSet]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataSet]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @tparam T The type of the resulting [[DataSet]].
    * @return The converted [[DataSet]].
    */
  def toDataSetScala[T: TypeInformation](table: Table): ScalaDataSet[T]

  /**
    * Converts the given [[Table]] into a [[DataSet]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataSet]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataSet]] types: Fields are mapped by field name, field types must match.
    *
    * @param table       The [[Table]] to convert.
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the resulting [[DataSet]].
    * @return The converted [[DataSet]].
    */
  def toDataSetScala[T: TypeInformation](
                                          table: Table,
                                          queryConfig: BatchQueryConfig): ScalaDataSet[T]

  /**
    * Registers a [[TableFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param tf   The TableFunction to register.
    * @tparam T The type of the output row.
    */
  def registerFunctionScala[T: TypeInformation](name: String, tf: TableFunction[T]): Unit

  /**
    * Registers an [[AggregateFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param f    The AggregateFunction to register.
    * @tparam T   The type of the output value.
    * @tparam ACC The type of aggregate accumulator.
    */
  def registerFunctionScala[T: TypeInformation, ACC: TypeInformation](
                                                                       name: String,
                                                                       f: AggregateFunction[T, ACC])
  : Unit

  /**
    * Converts the given [[DataSet]] into a [[Table]].
    *
    * The field names of the [[Table]] are automatically derived from the type of the [[DataSet]].
    *
    * @param dataSet The [[DataSet]] to be converted.
    * @tparam T The type of the [[DataSet]].
    * @return The converted [[Table]].
    */
  def fromDataSet[T](dataSet: DataSet[T]): Table

  /**
    * Converts the given [[DataSet]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   DataSet<Tuple2<String, Long>> set = ...
    *   Table tab = tableEnv.fromDataSet(set, "a, b")
    * }}}
    *
    * @param dataSet The [[DataSet]] to be converted.
    * @param fields  The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataSet]].
    * @return The converted [[Table]].
    */
  def fromDataSet[T](dataSet: DataSet[T], fields: String): Table

  /**
    * Registers the given [[DataSet]] as table in the
    * [[AbstractTableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived from the type of the [[DataSet]].
    *
    * @param name    The name under which the [[DataSet]] is registered in the catalog.
    * @param dataSet The [[DataSet]] to register.
    * @tparam T The type of the [[DataSet]] to register.
    */
  def registerDataSet[T](name: String, dataSet: DataSet[T]): Unit

  /**
    * Registers the given [[DataSet]] as table with specified field names in the
    * [[AbstractTableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   DataSet<Tuple2<String, Long>> set = ...
    *   tableEnv.registerDataSet("myTable", set, "a, b")
    * }}}
    *
    * @param name    The name under which the [[DataSet]] is registered in the catalog.
    * @param dataSet The [[DataSet]] to register.
    * @param fields  The field names of the registered table.
    * @tparam T The type of the [[DataSet]] to register.
    */
  def registerDataSet[T](name: String, dataSet: DataSet[T], fields: String): Unit

  /**
    * Converts the given [[Table]] into a [[DataSet]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataSet]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataSet]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @param clazz The class of the type of the resulting [[DataSet]].
    * @tparam T The type of the resulting [[DataSet]].
    * @return The converted [[DataSet]].
    */
  def toDataSet[T](table: Table, clazz: Class[T]): DataSet[T]

  /**
    * Converts the given [[Table]] into a [[DataSet]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataSet]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataSet]] types: Fields are mapped by field name, field types must match.
    *
    * @param table    The [[Table]] to convert.
    * @param typeInfo The [[TypeInformation]] that specifies the type of the resulting [[DataSet]].
    * @tparam T The type of the resulting [[DataSet]].
    * @return The converted [[DataSet]].
    */
  def toDataSet[T](table: Table, typeInfo: TypeInformation[T]): DataSet[T]

  /**
    * Converts the given [[Table]] into a [[DataSet]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataSet]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataSet]] types: Fields are mapped by field name, field types must match.
    *
    * @param table       The [[Table]] to convert.
    * @param clazz       The class of the type of the resulting [[DataSet]].
    * @param queryConfig The configuration for the query to generate.
    * @tparam T The type of the resulting [[DataSet]].
    * @return The converted [[DataSet]].
    */
  def toDataSet[T](
                    table: Table,
                    clazz: Class[T],
                    queryConfig: BatchQueryConfig): DataSet[T]

  /**
    * Converts the given [[Table]] into a [[DataSet]] of a specified type.
    *
    * The fields of the [[Table]] are mapped to [[DataSet]] fields as follows:
    * - [[org.apache.flink.types.Row]] and [[org.apache.flink.api.java.tuple.Tuple]]
    * types: Fields are mapped by position, field types must match.
    * - POJO [[DataSet]] types: Fields are mapped by field name, field types must match.
    *
    * @param table       The [[Table]] to convert.
    * @param typeInfo    The [[TypeInformation]] that specifies the type of the resulting [[DataSet]].
    * @param queryConfig The configuration for the query to generate.
    * @tparam T The type of the resulting [[DataSet]].
    * @return The converted [[DataSet]].
    */
  def toDataSet[T](
                    table: Table,
                    typeInfo: TypeInformation[T],
                    queryConfig: BatchQueryConfig): DataSet[T]

  /**
    * Registers a [[TableFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param tf   The TableFunction to register.
    * @tparam T The type of the output row.
    */
  def registerFunction[T](name: String, tf: TableFunction[T]): Unit

  /**
    * Registers an [[AggregateFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param f    The AggregateFunction to register.
    * @tparam T   The type of the output value.
    * @tparam ACC The type of aggregate accumulator.
    */
  def registerFunction[T, ACC](
                                name: String,
                                f: AggregateFunction[T, ACC]): Unit

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
  def fromDataStream[T](dataStream: ScalaDataStream[T]): Table

  /**
    * Converts the given [[DataStream]] into a [[Table]] with specified field names.
    *
    * Example:
    *
    * {{{
    *   val stream: DataStream[(String, Long)] = ...
    *   val tab: Table = tableEnv.fromDataStream(stream, 'a, 'b)
    * }}}
    *
    * @param dataStream The [[DataStream]] to be converted.
    * @param fields     The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromDataStream[T](dataStream: ScalaDataStream[T], fields: Expression*): Table

  /**
    * Registers the given [[DataStream]] as table in the
    * [[AbstractTableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived
    * from the type of the [[DataStream]].
    *
    * @param name       The name under which the [[DataStream]] is registered in the catalog.
    * @param dataStream The [[DataStream]] to register.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerDataStream[T](name: String, dataStream: ScalaDataStream[T]): Unit

  /**
    * Registers the given [[DataStream]] as table with specified field names in the
    * [[AbstractTableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   val set: DataStream[(String, Long)] = ...
    *   tableEnv.registerDataStream("myTable", set, 'a, 'b)
    * }}}
    *
    * @param name       The name under which the [[DataStream]] is registered in the catalog.
    * @param dataStream The [[DataStream]] to register.
    * @param fields     The field names of the registered table.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerDataStream[T](
                             name: String,
                             dataStream: ScalaDataStream[T],
                             fields: Expression*): Unit

  /**
    * Converts the given [[Table]] into an append [[DataStream]] of a specified type.
    *
    * The [[Table]] must only have insert (append) changes. If the [[Table]] is also modified
    * by update or delete changes, the conversion will fail.
    *
    * The fields of the [[Table]] are mapped to [[DataStream]] fields as follows:
    * - [[org.apache.flink.types.Row]] and Scala Tuple types: Fields are mapped by position, field
    * types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param table The [[Table]] to convert.
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toAppendStreamScala[T: TypeInformation](table: Table): ScalaDataStream[T]

  /**
    * Converts the given [[Table]] into an append [[DataStream]] of a specified type.
    *
    * The [[Table]] must only have insert (append) changes. If the [[Table]] is also modified
    * by update or delete changes, the conversion will fail.
    *
    * The fields of the [[Table]] are mapped to [[DataStream]] fields as follows:
    * - [[org.apache.flink.types.Row]] and Scala Tuple types: Fields are mapped by position, field
    * types must match.
    * - POJO [[DataStream]] types: Fields are mapped by field name, field types must match.
    *
    * @param table       The [[Table]] to convert.
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toAppendStreamScala[T: TypeInformation](
                                               table: Table,
                                               queryConfig: StreamQueryConfig): ScalaDataStream[T]

  /**
    * Converts the given [[Table]] into a [[DataStream]] of add and retract messages.
    * The message will be encoded as [[Tuple2]]. The first field is a [[Boolean]] flag,
    * the second field holds the record of the specified type [[T]].
    *
    * A true [[Boolean]] flag indicates an add message, a false flag indicates a retract message.
    *
    * @param table The [[Table]] to convert.
    * @tparam T The type of the requested data type.
    * @return The converted [[DataStream]].
    */
  def toRetractStreamScala[T: TypeInformation](table: Table): ScalaDataStream[(Boolean, T)]

  /**
    * Converts the given [[Table]] into a [[DataStream]] of add and retract messages.
    * The message will be encoded as [[Tuple2]]. The first field is a [[Boolean]] flag,
    * the second field holds the record of the specified type [[T]].
    *
    * A true [[Boolean]] flag indicates an add message, a false flag indicates a retract message.
    *
    * @param table       The [[Table]] to convert.
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the requested data type.
    * @return The converted [[DataStream]].
    */
  def toRetractStreamScala[T: TypeInformation](
                                                table: Table,
                                                queryConfig: StreamQueryConfig): ScalaDataStream[(Boolean, T)]

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
  def fromDataStream[T](dataStream: DataStream[T]): Table

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
    * @param fields     The field names of the resulting [[Table]].
    * @tparam T The type of the [[DataStream]].
    * @return The converted [[Table]].
    */
  def fromDataStream[T](dataStream: DataStream[T], fields: String): Table

  /**
    * Registers the given [[DataStream]] as table in the
    * [[AbstractTableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * The field names of the [[Table]] are automatically derived
    * from the type of the [[DataStream]].
    *
    * @param name       The name under which the [[DataStream]] is registered in the catalog.
    * @param dataStream The [[DataStream]] to register.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerDataStream[T](name: String, dataStream: DataStream[T]): Unit

  /**
    * Registers the given [[DataStream]] as table with specified field names in the
    * [[AbstractTableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * Example:
    *
    * {{{
    *   DataStream<Tuple2<String, Long>> set = ...
    *   tableEnv.registerDataStream("myTable", set, "a, b")
    * }}}
    *
    * @param name       The name under which the [[DataStream]] is registered in the catalog.
    * @param dataStream The [[DataStream]] to register.
    * @param fields     The field names of the registered table.
    * @tparam T The type of the [[DataStream]] to register.
    */
  def registerDataStream[T](name: String, dataStream: DataStream[T], fields: String): Unit

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
  def toAppendStream[T](table: Table, clazz: Class[T]): DataStream[T]

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
    * @param table    The [[Table]] to convert.
    * @param typeInfo The [[TypeInformation]] that specifies the type of the [[DataStream]].
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toAppendStream[T](table: Table, typeInfo: TypeInformation[T]): DataStream[T]

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
    * @param table       The [[Table]] to convert.
    * @param clazz       The class of the type of the resulting [[DataStream]].
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toAppendStream[T](
                         table: Table,
                         clazz: Class[T],
                         queryConfig: StreamQueryConfig): DataStream[T]

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
    * @param table       The [[Table]] to convert.
    * @param typeInfo    The [[TypeInformation]] that specifies the type of the [[DataStream]].
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the resulting [[DataStream]].
    * @return The converted [[DataStream]].
    */
  def toAppendStream[T](
                         table: Table,
                         typeInfo: TypeInformation[T],
                         queryConfig: StreamQueryConfig): DataStream[T]

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
                          clazz: Class[T]): DataStream[JTuple2[JBool, T]]

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
    * @param table    The [[Table]] to convert.
    * @param typeInfo The [[TypeInformation]] of the requested record type.
    * @tparam T The type of the requested record type.
    * @return The converted [[DataStream]].
    */
  def toRetractStream[T](
                          table: Table,
                          typeInfo: TypeInformation[T]): DataStream[JTuple2[JBool, T]]

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
    * @param table       The [[Table]] to convert.
    * @param clazz       The class of the requested record type.
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the requested record type.
    * @return The converted [[DataStream]].
    */
  def toRetractStream[T](
                          table: Table,
                          clazz: Class[T],
                          queryConfig: StreamQueryConfig): DataStream[JTuple2[JBool, T]]

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
    * @param table       The [[Table]] to convert.
    * @param typeInfo    The [[TypeInformation]] of the requested record type.
    * @param queryConfig The configuration of the query to generate.
    * @tparam T The type of the requested record type.
    * @return The converted [[DataStream]].
    */
  def toRetractStream[T](
      table: Table,
      typeInfo: TypeInformation[T],
      queryConfig: StreamQueryConfig): DataStream[JTuple2[JBool, T]]

}

/**
  * Object to instantiate a [[TableEnvironment]] depending on the batch or stream execution
  * environment.
  */
object TableEnvironment {

  /**
    * Returns a [[BatchTableEnvironment]] for a Java [[JavaBatchExecEnv]].
    *
    * @param executionEnvironment The Java batch ExecutionEnvironment.
    */
  def getTableEnvironment(executionEnvironment: JavaBatchExecEnv): BatchTableEnvironment = {
    new BatchTableEnvironment(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[BatchTableEnvironment]] for a Java [[JavaBatchExecEnv]]
    * and a given [[TableConfig]].
    *
    * @param executionEnvironment The Java batch ExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getTableEnvironment(
      executionEnvironment: JavaBatchExecEnv,
      tableConfig: TableConfig): BatchTableEnvironment = {
    new BatchTableEnvironment(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[BatchTableEnvironment]] for a Scala [[ScalaBatchExecEnv]].
    *
    * @param executionEnvironment The Scala batch ExecutionEnvironment.
    */
  def getTableEnvironment(executionEnvironment: ScalaBatchExecEnv)
  : BatchTableEnvironment = {
    new BatchTableEnvironment(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[BatchTableEnvironment]] for a Scala [[ScalaBatchExecEnv]] and a given
    * [[TableConfig]].
    *
    * @param executionEnvironment The Scala batch ExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getTableEnvironment(
      executionEnvironment: ScalaBatchExecEnv,
      tableConfig: TableConfig): BatchTableEnvironment = {
    new BatchTableEnvironment(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[StreamTableEnvironment]] for a Java [[JavaStreamExecEnv]].
    *
    * @param executionEnvironment The Java StreamExecutionEnvironment.
    */
  def getTableEnvironment(executionEnvironment: JavaStreamExecEnv)
  : StreamTableEnvironment = {
    new StreamTableEnvironment(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[StreamTableEnvironment]] for a Java [[JavaStreamExecEnv]]
    * and a given [[TableConfig]].
    *
    * @param executionEnvironment The Java StreamExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getTableEnvironment(
      executionEnvironment: JavaStreamExecEnv,
      tableConfig: TableConfig): StreamTableEnvironment = {
    new StreamTableEnvironment(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[StreamTableEnvironment]] for a Scala stream [[ScalaStreamExecEnv]].
    *
    * @param executionEnvironment The Scala StreamExecutionEnvironment.
    */
  def getTableEnvironment(executionEnvironment: ScalaStreamExecEnv)
  : StreamTableEnvironment = {
    new StreamTableEnvironment(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[StreamTableEnvironment]] for a Scala stream [[ScalaStreamExecEnv]].
    *
    * @param executionEnvironment The Scala StreamExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  def getTableEnvironment(
      executionEnvironment: ScalaStreamExecEnv,
      tableConfig: TableConfig): StreamTableEnvironment = {
    new StreamTableEnvironment(executionEnvironment, tableConfig)
  }

  /**
    * Returns field names for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation extract the field names.
    * @tparam A The type of the TypeInformation.
    * @return An array holding the field names
    */
  def getFieldNames[A](inputType: TypeInformation[A]): Array[String] = {
    validateType(inputType)

    val fieldNames: Array[String] = inputType match {
      case t: CompositeType[_] => t.getFieldNames
      case _: TypeInformation[_] => Array("f0")
    }

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    fieldNames
  }

  /**
    * Validate if class represented by the typeInfo is static and globally accessible
    * @param typeInfo type to check
    * @throws TableException if type does not meet these criteria
    */
  def validateType(typeInfo: TypeInformation[_]): Unit = {
    val clazz = typeInfo.getTypeClass
    if ((clazz.isMemberClass && !Modifier.isStatic(clazz.getModifiers)) ||
      !Modifier.isPublic(clazz.getModifiers) ||
      clazz.getCanonicalName == null) {
      throw new TableException(
        s"Class '$clazz' described in type information '$typeInfo' must be " +
          s"static and globally accessible.")
    }
  }

  /**
    * Returns field indexes for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation extract the field positions from.
    * @return An array holding the field positions
    */
  def getFieldIndices(inputType: TypeInformation[_]): Array[Int] = {
    getFieldNames(inputType).indices.toArray
  }

  /**
    * Returns field types for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation to extract field types from.
    * @return An array holding the field types.
    */
  def getFieldTypes(inputType: TypeInformation[_]): Array[TypeInformation[_]] = {
    validateType(inputType)

    inputType match {
      case ct: CompositeType[_] => 0.until(ct.getArity).map(i => ct.getTypeAt(i)).toArray
      case t: TypeInformation[_] => Array(t.asInstanceOf[TypeInformation[_]])
    }
  }
}
