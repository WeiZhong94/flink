/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.pickle;

import org.apache.flink.api.common.python.GlobalPythonScalarFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

/**
 * Tests for GlobalPythonScalarFunction.
 */
public class GlobalPythonScalarFunctionTest {

	@Test
	public void testGlobalPythonScalarFunction() throws Exception {
		try (OutputStream out = new FileOutputStream("./test1.py")) {
			String code = "def func1(str):\n    return str + str\n";
			out.write(code.getBytes());
		}
		ScalarFunction func = new GlobalPythonScalarFunction("test1.func1", Types.STRING);
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
		tEnv.registerFunction("func1", func);
		Table t = tEnv.fromDataSet(env.fromElements("1", "2", "3")).as("str").select("func1(str)");
		System.out.println(tEnv.toDataSet(t, String.class).collect());
		new File("./test1.py").delete();
	}
}
