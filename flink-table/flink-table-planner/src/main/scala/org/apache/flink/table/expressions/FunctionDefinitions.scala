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

package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}

trait FunctionDefinition

class BuildInFunctionDefinition(val name: String, val reuseJavaFunctionCatalog: Boolean = true)
  extends FunctionDefinition

class ScalarFunctionDefinition(val func: ScalarFunction) extends FunctionDefinition

class AggFunctionDefinition(
    val func: AggregateFunction[_, _],
    val resultTypeInfo: TypeInformation[_],
    val accTypeInfo: TypeInformation[_]) extends FunctionDefinition

object FunctionDefinitions {

  val CAST = new BuildInFunctionDefinition("cast", false)
  val FlATTENING = new BuildInFunctionDefinition("flattening", false)
  val GET_COMPOSITE_FIELD = new BuildInFunctionDefinition("getCompositeField", false)
  val IN = new BuildInFunctionDefinition("in", false)


  //function in catalog
  val AND = new BuildInFunctionDefinition("and")
  val OR = new BuildInFunctionDefinition("or")
  val NOT = new BuildInFunctionDefinition("not")
  val EQUALS = new BuildInFunctionDefinition("equals")
  val GREATER_THAN = new BuildInFunctionDefinition("greaterThan")
  val GREATER_THAN_OR_EQUAL = new BuildInFunctionDefinition("greaterThanOrEqual")
  val LESS_THAN = new BuildInFunctionDefinition("lessThan")
  val LESS_THAN_OR_EQUAL = new BuildInFunctionDefinition("lessThanOrEqual")
  val NOT_EQUALS = new BuildInFunctionDefinition("notEquals")
  //val IN = new BuildInFunctionDefinition("in")
  val IS_NULL = new BuildInFunctionDefinition("isNull")
  val IS_NOT_NULL = new BuildInFunctionDefinition("isNotNull")
  val IS_TRUE = new BuildInFunctionDefinition("isTrue")
  val IS_FALSE = new BuildInFunctionDefinition("isFalse")
  val IS_NOT_TRUE = new BuildInFunctionDefinition("isNotTrue")
  val IS_NOT_FALSE = new BuildInFunctionDefinition("isNotFalse")
  val IF = new BuildInFunctionDefinition("if")
  val BETWEEN = new BuildInFunctionDefinition("between")
  val NOT_BETWEEN = new BuildInFunctionDefinition("notBetween")

  val AVG = new BuildInFunctionDefinition("avg")
  val COUNT = new BuildInFunctionDefinition("count")
  val MAX = new BuildInFunctionDefinition("max")
  val MIN = new BuildInFunctionDefinition("min")
  val SUM = new BuildInFunctionDefinition("sum")
  val SUM0 = new BuildInFunctionDefinition("sum0")
  val STDDEV_POP = new BuildInFunctionDefinition("stddevPop")
  val STDDEV_SAMP = new BuildInFunctionDefinition("stddevSamp")
  val VAR_POP = new BuildInFunctionDefinition("varPop")
  val VAR_SAMP = new BuildInFunctionDefinition("varSamp")
  val COLLECT = new BuildInFunctionDefinition("collect")

  val CHAR_LENGTH = new BuildInFunctionDefinition("charLength")
  val INIT_CAP = new BuildInFunctionDefinition("initCap")
  val LIKE = new BuildInFunctionDefinition("like")
  //val CONCAT = new BuildInFunctionDefinition("concat")
  val LOWER = new BuildInFunctionDefinition("lower")
  val LOWER_CASE = new BuildInFunctionDefinition("lowerCase")
  val SIMILAR = new BuildInFunctionDefinition("similar")
  val SUBSTRING = new BuildInFunctionDefinition("substring")
  val REPLACE = new BuildInFunctionDefinition("replace")
  val TRIM = new BuildInFunctionDefinition("trim")
  val UPPER = new BuildInFunctionDefinition("upper")
  val UPPER_CASE = new BuildInFunctionDefinition("upperCase")
  val POSITION = new BuildInFunctionDefinition("position")
  val OVERLAY = new BuildInFunctionDefinition("overlay")
  val CONCAT = new BuildInFunctionDefinition("concat")
  val CONCAT_WS = new BuildInFunctionDefinition("concat_ws")
  val LPAD = new BuildInFunctionDefinition("lpad")
  val RPAD = new BuildInFunctionDefinition("rpad")
  val REGEXP_EXTRACT = new BuildInFunctionDefinition("regexpExtract")
  val FROM_BASE64 = new BuildInFunctionDefinition("fromBase64")
  val TO_BASE64 = new BuildInFunctionDefinition("toBase64")
  val UUID = new BuildInFunctionDefinition("uuid")
  val LTRIM = new BuildInFunctionDefinition("ltrim")
  val RTRIM = new BuildInFunctionDefinition("rtrim")
  val REPEAT = new BuildInFunctionDefinition("repeat")
  val REGEXP_REPLACE = new BuildInFunctionDefinition("regexpReplace")

  val PLUS = new BuildInFunctionDefinition("plus")
  val MINUS = new BuildInFunctionDefinition("minus")
  val DIVIDE = new BuildInFunctionDefinition("divide")
  val TIMES = new BuildInFunctionDefinition("times")
  val ABS = new BuildInFunctionDefinition("abs")
  val CEIL = new BuildInFunctionDefinition("ceil")
  val EXP = new BuildInFunctionDefinition("exp")
  val FLOOR = new BuildInFunctionDefinition("floor")
  val LOG10 = new BuildInFunctionDefinition("log10")
  val LOG2 = new BuildInFunctionDefinition("log2")
  val LN = new BuildInFunctionDefinition("ln")
  val LOG = new BuildInFunctionDefinition("log")
  val POWER = new BuildInFunctionDefinition("power")
  val MOD = new BuildInFunctionDefinition("mod")
  val SQRT = new BuildInFunctionDefinition("sqrt")
  val MINUS_PREFIX = new BuildInFunctionDefinition("minusPrefix")
  val SIN = new BuildInFunctionDefinition("sin")
  val COS = new BuildInFunctionDefinition("cos")
  val SINH = new BuildInFunctionDefinition("sinh")
  val TAN = new BuildInFunctionDefinition("tan")
  val TANH = new BuildInFunctionDefinition("tanh")
  val COT = new BuildInFunctionDefinition("cot")
  val ASIN = new BuildInFunctionDefinition("asin")
  val ACOS = new BuildInFunctionDefinition("acos")
  val ATAN = new BuildInFunctionDefinition("atan")
  val ATAN2 = new BuildInFunctionDefinition("atan2")
  val COSH = new BuildInFunctionDefinition("cosh")
  val DEGREES = new BuildInFunctionDefinition("degrees")
  val RADIANS = new BuildInFunctionDefinition("radians")
  val SIGN = new BuildInFunctionDefinition("sign")
  val ROUND = new BuildInFunctionDefinition("round")
  val PI = new BuildInFunctionDefinition("pi")
  val E = new BuildInFunctionDefinition("e")
  val RAND = new BuildInFunctionDefinition("rand")
  val RAND_INTEGER = new BuildInFunctionDefinition("randInteger")
  val BIN = new BuildInFunctionDefinition("bin")
  val HEX = new BuildInFunctionDefinition("hex")
  val TRUNCATE = new BuildInFunctionDefinition("truncate")

  val EXTRACT = new BuildInFunctionDefinition("extract")
  val CURRENT_DATE = new BuildInFunctionDefinition("currentDate")
  val CURRENT_TIME = new BuildInFunctionDefinition("currentTime")
  val CURRENT_TIMESTAMP = new BuildInFunctionDefinition("currentTimestamp")
  val LOCAL_TIME = new BuildInFunctionDefinition("localTime")
  val LOCAL_TIMESTAMP = new BuildInFunctionDefinition("localTimestamp")
  val QUARTER = new BuildInFunctionDefinition("quarter")
  val TEMPORAL_OVERLAPS = new BuildInFunctionDefinition("temporalOverlaps")
  val DATE_TIME_PLUS = new BuildInFunctionDefinition("dateTimePlus")
  val DATE_FORMAT = new BuildInFunctionDefinition("dateFormat")
  val TIMESTAMP_DIFF = new BuildInFunctionDefinition("timestampDiff")
  val TEMPORAL_FLOOR = new BuildInFunctionDefinition("temporalFloor")
  val TEMPORAL_CEIL = new BuildInFunctionDefinition("temporalCeil")

  val AT = new BuildInFunctionDefinition("at")

  val CARDINALITY = new BuildInFunctionDefinition("cardinality")

  val ARRAY = new BuildInFunctionDefinition("array")
  val ELEMENT = new BuildInFunctionDefinition("element")

  val MAP = new BuildInFunctionDefinition("map")

  val ROW = new BuildInFunctionDefinition("row")

  val START = new BuildInFunctionDefinition("start")
  val END = new BuildInFunctionDefinition("end")

  val ASC = new BuildInFunctionDefinition("asc")
  val DESC = new BuildInFunctionDefinition("desc")

  val MD5 = new BuildInFunctionDefinition("md5")
  val SHA1 = new BuildInFunctionDefinition("sha1")
  val SHA224 = new BuildInFunctionDefinition("sha224")
  val SHA256 = new BuildInFunctionDefinition("sha256")
  val SHA384 = new BuildInFunctionDefinition("sha384")
  val SHA512 = new BuildInFunctionDefinition("sha512")
  val SHA2 = new BuildInFunctionDefinition("sha2")

}
