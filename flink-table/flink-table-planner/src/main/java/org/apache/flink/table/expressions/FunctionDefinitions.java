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

package org.apache.flink.table.expressions;

/**
 * FunctionDefinitions.
 */
public class FunctionDefinitions {

	public static final FunctionDefinition CAST = new BuildInFunctionDefinition("cast", false);
	public static final FunctionDefinition FLATTENING = new BuildInFunctionDefinition("flattening", false);
	public static final FunctionDefinition GET_COMPOSITE_FIELD = new BuildInFunctionDefinition("getCompositeField", false);
	public static final FunctionDefinition IN = new BuildInFunctionDefinition("in", false);


	//function in catalog
	public static final FunctionDefinition AND = new BuildInFunctionDefinition("and");
	public static final FunctionDefinition OR = new BuildInFunctionDefinition("or");
	public static final FunctionDefinition NOT = new BuildInFunctionDefinition("not");
	public static final FunctionDefinition EQUALS = new BuildInFunctionDefinition("equals");
	public static final FunctionDefinition GREATER_THAN = new BuildInFunctionDefinition("greaterThan");
	public static final FunctionDefinition GREATER_THAN_OR_EQUAL = new BuildInFunctionDefinition("greaterThanOrEqual");
	public static final FunctionDefinition LESS_THAN = new BuildInFunctionDefinition("lessThan");
	public static final FunctionDefinition LESS_THAN_OR_EQUAL = new BuildInFunctionDefinition("lessThanOrEqual");
	public static final FunctionDefinition NOT_EQUALS = new BuildInFunctionDefinition("notEquals");
	//public static final FunctionDefinition IN = new BuildInFunctionDefinition("in");
	public static final FunctionDefinition IS_NULL = new BuildInFunctionDefinition("isNull");
	public static final FunctionDefinition IS_NOT_NULL = new BuildInFunctionDefinition("isNotNull");
	public static final FunctionDefinition IS_TRUE = new BuildInFunctionDefinition("isTrue");
	public static final FunctionDefinition IS_FALSE = new BuildInFunctionDefinition("isFalse");
	public static final FunctionDefinition IS_NOT_TRUE = new BuildInFunctionDefinition("isNotTrue");
	public static final FunctionDefinition IS_NOT_FALSE = new BuildInFunctionDefinition("isNotFalse");
	public static final FunctionDefinition IF = new BuildInFunctionDefinition("if");
	public static final FunctionDefinition BETWEEN = new BuildInFunctionDefinition("between");
	public static final FunctionDefinition NOT_BETWEEN = new BuildInFunctionDefinition("notBetween");

	public static final FunctionDefinition AVG = new BuildInFunctionDefinition("avg");
	public static final FunctionDefinition COUNT = new BuildInFunctionDefinition("count");
	public static final FunctionDefinition MAX = new BuildInFunctionDefinition("max");
	public static final FunctionDefinition MIN = new BuildInFunctionDefinition("min");
	public static final FunctionDefinition SUM = new BuildInFunctionDefinition("sum");
	public static final FunctionDefinition SUM0 = new BuildInFunctionDefinition("sum0");
	public static final FunctionDefinition STDDEV_POP = new BuildInFunctionDefinition("stddevPop");
	public static final FunctionDefinition STDDEV_SAMP = new BuildInFunctionDefinition("stddevSamp");
	public static final FunctionDefinition VAR_POP = new BuildInFunctionDefinition("varPop");
	public static final FunctionDefinition VAR_SAMP = new BuildInFunctionDefinition("varSamp");
	public static final FunctionDefinition COLLECT = new BuildInFunctionDefinition("collect");

	public static final FunctionDefinition CHAR_LENGTH = new BuildInFunctionDefinition("charLength");
	public static final FunctionDefinition INIT_CAP = new BuildInFunctionDefinition("initCap");
	public static final FunctionDefinition LIKE = new BuildInFunctionDefinition("like");
	//public static final FunctionDefinition CONCAT = new BuildInFunctionDefinition("concat");
	public static final FunctionDefinition LOWER = new BuildInFunctionDefinition("lower");
	//public static final FunctionDefinition LOWER_CASE = new BuildInFunctionDefinition("lowerCase");
	public static final FunctionDefinition SIMILAR = new BuildInFunctionDefinition("similar");
	public static final FunctionDefinition SUBSTRING = new BuildInFunctionDefinition("substring");
	public static final FunctionDefinition REPLACE = new BuildInFunctionDefinition("replace");
	public static final FunctionDefinition TRIM = new BuildInFunctionDefinition("trim");
	public static final FunctionDefinition UPPER = new BuildInFunctionDefinition("upper");
	//public static final FunctionDefinition UPPER_CASE = new BuildInFunctionDefinition("upperCase");
	public static final FunctionDefinition POSITION = new BuildInFunctionDefinition("position");
	public static final FunctionDefinition OVERLAY = new BuildInFunctionDefinition("overlay");
	public static final FunctionDefinition CONCAT = new BuildInFunctionDefinition("concat");
	public static final FunctionDefinition CONCAT_WS = new BuildInFunctionDefinition("concat_ws");
	public static final FunctionDefinition LPAD = new BuildInFunctionDefinition("lpad");
	public static final FunctionDefinition RPAD = new BuildInFunctionDefinition("rpad");
	public static final FunctionDefinition REGEXP_EXTRACT = new BuildInFunctionDefinition("regexpExtract");
	public static final FunctionDefinition FROM_BASE64 = new BuildInFunctionDefinition("fromBase64");
	public static final FunctionDefinition TO_BASE64 = new BuildInFunctionDefinition("toBase64");
	public static final FunctionDefinition UUID = new BuildInFunctionDefinition("uuid");
	public static final FunctionDefinition LTRIM = new BuildInFunctionDefinition("ltrim");
	public static final FunctionDefinition RTRIM = new BuildInFunctionDefinition("rtrim");
	public static final FunctionDefinition REPEAT = new BuildInFunctionDefinition("repeat");
	public static final FunctionDefinition REGEXP_REPLACE = new BuildInFunctionDefinition("regexpReplace");

	public static final FunctionDefinition PLUS = new BuildInFunctionDefinition("plus");
	public static final FunctionDefinition MINUS = new BuildInFunctionDefinition("minus");
	public static final FunctionDefinition DIVIDE = new BuildInFunctionDefinition("divide");
	public static final FunctionDefinition TIMES = new BuildInFunctionDefinition("times");
	public static final FunctionDefinition ABS = new BuildInFunctionDefinition("abs");
	public static final FunctionDefinition CEIL = new BuildInFunctionDefinition("ceil");
	public static final FunctionDefinition EXP = new BuildInFunctionDefinition("exp");
	public static final FunctionDefinition FLOOR = new BuildInFunctionDefinition("floor");
	public static final FunctionDefinition LOG10 = new BuildInFunctionDefinition("log10");
	public static final FunctionDefinition LOG2 = new BuildInFunctionDefinition("log2");
	public static final FunctionDefinition LN = new BuildInFunctionDefinition("ln");
	public static final FunctionDefinition LOG = new BuildInFunctionDefinition("log");
	public static final FunctionDefinition POWER = new BuildInFunctionDefinition("power");
	public static final FunctionDefinition MOD = new BuildInFunctionDefinition("mod");
	public static final FunctionDefinition SQRT = new BuildInFunctionDefinition("sqrt");
	public static final FunctionDefinition MINUS_PREFIX = new BuildInFunctionDefinition("minusPrefix");
	public static final FunctionDefinition SIN = new BuildInFunctionDefinition("sin");
	public static final FunctionDefinition COS = new BuildInFunctionDefinition("cos");
	public static final FunctionDefinition SINH = new BuildInFunctionDefinition("sinh");
	public static final FunctionDefinition TAN = new BuildInFunctionDefinition("tan");
	public static final FunctionDefinition TANH = new BuildInFunctionDefinition("tanh");
	public static final FunctionDefinition COT = new BuildInFunctionDefinition("cot");
	public static final FunctionDefinition ASIN = new BuildInFunctionDefinition("asin");
	public static final FunctionDefinition ACOS = new BuildInFunctionDefinition("acos");
	public static final FunctionDefinition ATAN = new BuildInFunctionDefinition("atan");
	public static final FunctionDefinition ATAN2 = new BuildInFunctionDefinition("atan2");
	public static final FunctionDefinition COSH = new BuildInFunctionDefinition("cosh");
	public static final FunctionDefinition DEGREES = new BuildInFunctionDefinition("degrees");
	public static final FunctionDefinition RADIANS = new BuildInFunctionDefinition("radians");
	public static final FunctionDefinition SIGN = new BuildInFunctionDefinition("sign");
	public static final FunctionDefinition ROUND = new BuildInFunctionDefinition("round");
	public static final FunctionDefinition PI = new BuildInFunctionDefinition("pi");
	public static final FunctionDefinition E = new BuildInFunctionDefinition("e");
	public static final FunctionDefinition RAND = new BuildInFunctionDefinition("rand");
	public static final FunctionDefinition RAND_INTEGER = new BuildInFunctionDefinition("randInteger");
	public static final FunctionDefinition BIN = new BuildInFunctionDefinition("bin");
	public static final FunctionDefinition HEX = new BuildInFunctionDefinition("hex");
	public static final FunctionDefinition TRUNCATE = new BuildInFunctionDefinition("truncate");

	public static final FunctionDefinition EXTRACT = new BuildInFunctionDefinition("extract");
	public static final FunctionDefinition CURRENT_DATE = new BuildInFunctionDefinition("currentDate");
	public static final FunctionDefinition CURRENT_TIME = new BuildInFunctionDefinition("currentTime");
	public static final FunctionDefinition CURRENT_TIMESTAMP = new BuildInFunctionDefinition("currentTimestamp");
	public static final FunctionDefinition LOCAL_TIME = new BuildInFunctionDefinition("localTime");
	public static final FunctionDefinition LOCAL_TIMESTAMP = new BuildInFunctionDefinition("localTimestamp");
	public static final FunctionDefinition QUARTER = new BuildInFunctionDefinition("quarter");
	public static final FunctionDefinition TEMPORAL_OVERLAPS = new BuildInFunctionDefinition("temporalOverlaps");
	public static final FunctionDefinition DATE_TIME_PLUS = new BuildInFunctionDefinition("dateTimePlus");
	public static final FunctionDefinition DATE_FORMAT = new BuildInFunctionDefinition("dateFormat");
	public static final FunctionDefinition TIMESTAMP_DIFF = new BuildInFunctionDefinition("timestampDiff");
	public static final FunctionDefinition TEMPORAL_FLOOR = new BuildInFunctionDefinition("temporalFloor");
	public static final FunctionDefinition TEMPORAL_CEIL = new BuildInFunctionDefinition("temporalCeil");

	public static final FunctionDefinition AT = new BuildInFunctionDefinition("at");

	public static final FunctionDefinition CARDINALITY = new BuildInFunctionDefinition("cardinality");

	public static final FunctionDefinition ARRAY = new BuildInFunctionDefinition("array");
	public static final FunctionDefinition ELEMENT = new BuildInFunctionDefinition("element");

	public static final FunctionDefinition MAP = new BuildInFunctionDefinition("map");

	public static final FunctionDefinition ROW = new BuildInFunctionDefinition("row");

	public static final FunctionDefinition START = new BuildInFunctionDefinition("start");
	public static final FunctionDefinition END = new BuildInFunctionDefinition("end");

	public static final FunctionDefinition ASC = new BuildInFunctionDefinition("asc");
	public static final FunctionDefinition DESC = new BuildInFunctionDefinition("desc");

	public static final FunctionDefinition MD5 = new BuildInFunctionDefinition("md5");
	public static final FunctionDefinition SHA1 = new BuildInFunctionDefinition("sha1");
	public static final FunctionDefinition SHA224 = new BuildInFunctionDefinition("sha224");
	public static final FunctionDefinition SHA256 = new BuildInFunctionDefinition("sha256");
	public static final FunctionDefinition SHA384 = new BuildInFunctionDefinition("sha384");
	public static final FunctionDefinition SHA512 = new BuildInFunctionDefinition("sha512");
	public static final FunctionDefinition SHA2 = new BuildInFunctionDefinition("sha2");

}
