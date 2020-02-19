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

package org.apache.flink.client.python;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.python.PythonFunction;

import java.io.IOException;

import static org.apache.flink.client.python.PythonDriverEnvUtils.PYFLINK_CLIENT_EXECUTABLE;
import static org.apache.flink.client.python.PythonDriverEnvUtils.loadConfiguration;
import static org.apache.flink.python.PythonOptions.PYTHON_CLIENT_EXECUTABLE;
import static org.apache.flink.python.PythonOptions.PYTHON_CLIENT_PYTHONPATH;

/**
 * The factory which creates the PythonFunction objects from given module name and object name.
 */
public interface PythonFunctionFactory {

	/**
	 * Returns PythonFunction according to moduleName and objectName. The current environment is also
	 * needed because different environments have different code generation logic.
	 *
	 * @param moduleName The module name of the Python UDF.
	 * @param objectName The function name / class name of the Python UDF.
	 * @param environment The TableEnvironment to which the generated PythonFunction is registered.
	 * @return The PythonFunction object which represents the Python UDF.
	 */
	PythonFunction getPythonFunction(String moduleName, String objectName, TableEnvironment environment);

	/**
	 * Returns PythonFunction according to the fully qualified name of the Python UDF
	 * i.e ${moduleName}.${functionName} or ${moduleName}.${className}. The current environment is also
	 * needed because different environments have different code generation logic.
	 *
	 * @param fullyQualifiedName The fully qualified name of the Python UDF.
	 * @param environment The TableEnvironment to which the generated PythonFunction is registered.
	 * @return The PythonFunction object which represents the Python UDF.
	 */
	static PythonFunction getPythonFunction(String fullyQualifiedName, TableEnvironment environment)
			throws IOException {
		int splitIndex = fullyQualifiedName.lastIndexOf(".");
		String moduleName = fullyQualifiedName.substring(0, splitIndex);
		String objectName = fullyQualifiedName.substring(splitIndex + 1);

		Configuration appConf = environment.getConfig().getConfiguration();
		String pythonExecutable = loadConfiguration(PYTHON_CLIENT_EXECUTABLE, PYFLINK_CLIENT_EXECUTABLE, appConf);
		String pythonPath = loadConfiguration(PYTHON_CLIENT_PYTHONPATH, null, appConf);
		PythonFunctionFactory pythonFunctionFactory = PythonFunctionFactoryUtil.getPythonFunctionFactory(
			pythonExecutable, pythonPath);
		return pythonFunctionFactory.getPythonFunction(
			moduleName, objectName, environment);
	}
}
