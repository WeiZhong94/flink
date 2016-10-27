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

package org.apache.flink.api.scala.batch.sql


import java.sql.{Date, Time, Timestamp}
import java.util

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.batch.sql.FilterITCase.MyHashCode
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.functions.{ScalarFunction, TableValuedFunction}
import org.apache.flink.api.table.{Row, TableEnvironment, ValidationException}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import org.apache.flink.api.table.expressions.utils.TableValuedFunction0

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

@RunWith(classOf[Parameterized])
class CalcITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testSelectStarFromTable(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"

    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSelectStarFromDataSet(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"

    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSimpleSelectAll(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT a, b, c FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"

    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSelectWithNaming(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT _1 as a, _2 as b FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" + "7,4\n" +
      "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" + "15,5\n" +
      "16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n"

    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidFields(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT a, foo FROM MyTable"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    tEnv.sql(sqlQuery)
  }

  @Test
  def testAllRejectingFilter(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable WHERE false"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAllPassingFilter(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable WHERE true"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" + "4,3,Hello world, " +
      "how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" + "7,4," +
      "Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" + "11,5," +
      "Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" + "15,5," +
      "Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" + "19," +
      "6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterOnString(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable WHERE c LIKE '%world%'"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "3,2,Hello world\n" + "4,3,Hello world, how are you?\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterOnInteger(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable WHERE MOD(a,2)=0"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "2,2,Hello\n" + "4,3,Hello world, how are you?\n" +
      "6,3,Luke Skywalker\n" + "8,4," + "Comment#2\n" + "10,4,Comment#4\n" +
      "12,5,Comment#6\n" + "14,5,Comment#8\n" + "16,6," +
      "Comment#10\n" + "18,6,Comment#12\n" + "20,6,Comment#14\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testDisjunctivePredicate(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable WHERE a < 2 OR a > 20"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "1,1,Hi\n" + "21,6,Comment#15\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterWithAnd(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable WHERE MOD(a,2)<>0 AND MOD(b,2)=0"

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)

    val result = tEnv.sql(sqlQuery)

    val expected = "3,2,Hello world\n" + "7,4,Comment#1\n" +
      "9,4,Comment#3\n" + "17,6,Comment#11\n" +
      "19,6,Comment#13\n" + "21,6,Comment#15\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAdvancedDataTypes(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT a, b, c, DATE '1984-07-12', TIME '14:34:24', " +
      "TIMESTAMP '1984-07-12 14:34:24' FROM MyTable"

    val ds = env.fromElements((
      Date.valueOf("1984-07-12"),
      Time.valueOf("14:34:24"),
      Timestamp.valueOf("1984-07-12 14:34:24")))
    tEnv.registerDataSet("MyTable", ds, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "1984-07-12,14:34:24,1984-07-12 14:34:24.0," +
      "1984-07-12,14:34:24,1984-07-12 14:34:24.0"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedScalarFunction(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    tEnv.registerFunction("hashCode",
      new org.apache.flink.api.java.batch.table.CalcITCase.OldHashCode)
    tEnv.registerFunction("hashCode", MyHashCode)

    val ds = env.fromElements("a", "b", "c")
    tEnv.registerDataSet("MyTable", ds, 'text)

    val result = tEnv.sql("SELECT hashCode(text) FROM MyTable")

    val expected = "97\n98\n99"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
  @Test
  def testUDTF(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)
    tEnv.registerFunction("split", new TableValuedFunction0())
    var sqlQuery = "SELECT MyTable.a, MyTable.b, t.s " +
      "FROM MyTable,LATERAL TABLE(split(c)) AS t(s)"
    val tab = tEnv.sql(sqlQuery)

    val results = tab.toDataSet[Row].collect()
    var expected = "1,1,Hi\n1,1,KEVIN\n2,2,Hello\n2,2,SUNNY\n4,3,LOVER\n4,3,PAN"
    TestBaseUtils.compareResultAsText(results.asJava, expected)

  }

  @Test
  def testUDTFWithFilter(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)
    tEnv.registerFunction("split", new TableValuedFunction0())
    var sqlQuery = "SELECT MyTable.a, MyTable.b, t.s " +
      "FROM MyTable,LATERAL TABLE(split(c)) AS t(s)" +
      "WHERE MyTable.a <4"
    val tab = tEnv.sql(sqlQuery)

    val results = tab.toDataSet[Row].collect()
    var expected = "1,1,Hi\n1,1,KEVIN\n2,2,Hello\n2,2,SUNNY"
    TestBaseUtils.compareResultAsText(results.asJava, expected)

  }

  @Test
  def testLeftJoinUDTF(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", ds)
    tEnv.registerFunction("split", new TableValuedFunction0())
    var sqlQuery = "SELECT MyTable.a, MyTable.b, t.s FROM " +
      "MyTable LEFT JOIN LATERAL TABLE(split(c)) AS t(s) ON TRUE"
    val tab = tEnv.sql(sqlQuery)

    val results = tab.toDataSet[Row].collect()
    var expected = "1,1,Hi\n1,1,KEVIN\n2,2,Hello" +
      "\n2,2,SUNNY\n3,2,null\n4,3,LOVER\n4,3,PAN"
    TestBaseUtils.compareResultAsText(results.asJava, expected)

  }
  @Test
  def testInnerJoinUDTF(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = getSmall4TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c,'d)
    tEnv.registerTable("MyTable", ds)
    tEnv.registerFunction("split", new TableValuedFunction0())
    var sqlQuery = "SELECT MyTable.a, MyTable.b, t.s FROM " +
      "MyTable JOIN LATERAL TABLE(split(c)) AS t(s) ON MyTable.d=t.s "
    val tab = tEnv.sql(sqlQuery)

    val results = tab.toDataSet[Row].collect()
    var expected = "1,1,KEVIN\n2,2,SUNNY"
    TestBaseUtils.compareResultAsText(results.asJava, expected)

  }

  def getSmall3TupleDataSet(env: ExecutionEnvironment): DataSet[(Int, Long, String)] = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Hi#KEVIN"))
    data.+=((2, 2L, "Hello#SUNNY"))
    data.+=((3, 2L, "Hello world"))
    data.+=((4, 3L, "PAN#LOVER"))
    env.fromCollection(data)
  }
  def getSmall4TupleDataSet(env: ExecutionEnvironment): DataSet[(Int, Long, String,String)] = {
    val data = new mutable.MutableList[(Int, Long, String,String)]
    data.+=((1, 1L, "Hi#KEVIN","KEVIN"))
    data.+=((2, 2L, "Hello#SUNNY","SUNNY"))
    data.+=((3, 2L, "Hello#world","a"))
    data.+=((4, 3L, "PAN#LOVER","a"))
    env.fromCollection(data)
  }
}
object FilterITCase {
  object MyHashCode extends ScalarFunction {
    def eval(s: String): Int = s.hashCode()
  }
}

object CalcITCase {

  @Parameterized.Parameters(name = "Execution mode = {0}, Table config = {1}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(TestExecutionMode.COLLECTION, TableProgramsTestBase.DEFAULT),
      Array(TestExecutionMode.COLLECTION, TableProgramsTestBase.NO_NULL)).asJava
  }
}
