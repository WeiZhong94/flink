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

package org.apache.flink.client.python;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

/**
 * Tests for PythonFunctionFactoryTest.
 */
public class PythonFunctionFactoryTest {

	@Test
	public void testPythonFunctionFactory() throws Exception {
		try (OutputStream out = new FileOutputStream("../test1.py")) {
			String code = ""
				+ "from pyflink.table.udf import udf\n"
				+ "from pyflink.table import DataTypes\n"
				+ "@udf(input_types=DataTypes.STRING(), result_type=DataTypes.STRING())\n"
				+ "def func1(str):\n"
				+ "    return str + str\n";
			out.write(code.getBytes());
		}
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
		//ScalarFunction func = (ScalarFunction) pythonFunctionFactory.getPythonFunction("test1", "func1", tEnv);
		//tEnv.registerFunction("func1", func);
		tEnv.getConfig().getConfiguration().set(PythonOptions.PYTHON_CLIENT_PYTHONPATH, "../");
		tEnv.sqlUpdate("create temporary system function func1 as 'test1.func1' language python");
		DependencyManager dependencyManager = DependencyManager.create(tEnv);
		dependencyManager.addPythonFile("../test1.py");
		Table t = tEnv.fromDataSet(env.fromElements("1", "2", "3")).as("str").select("func1(str)");
		//System.out.println(tEnv.toDataSet(t, String.class).collect());
		new File("../test1.py").delete();
	}
}
