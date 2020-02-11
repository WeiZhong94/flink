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

package org.apache.flink.api.common.python;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.descriptors.FunctionDescriptor;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;

import java.io.IOException;
import java.util.Map;

/**
 * GlobalPythonScalarFunction.
 */
public class GlobalPythonScalarFunction extends ScalarFunction implements PythonFunction {

	private byte[] serializedPythonFunction;
	private TypeInformation<?> resultType;
	private boolean deterministic;

	private GlobalPythonScalarFunction(byte[] serializedPythonFunction, TypeInformation<?> resultType, boolean deterministic) {
		this.serializedPythonFunction = serializedPythonFunction;
		this.resultType = resultType;
		this.deterministic = deterministic;
	}

	public Object eval(Object... args) {
		return null;
	}

	@Override
	public byte[] getSerializedPythonFunction() {
		return serializedPythonFunction;
	}

	@Override
	public PythonEnv getPythonEnv() {
		return new PythonEnv(PythonEnv.ExecType.PROCESS);
	}

	@Override
	public boolean isDeterministic() {
		return deterministic;
	}

	@Override
	public TypeInformation<?> getResultType(Class<?>[] signature) {
		return resultType;
	}

	public static GlobalPythonScalarFunction create(String fullName, TypeInformation<?> resultType) throws IOException {
		return create(fullName, resultType, true);
	}

	public static GlobalPythonScalarFunction create(String fullName, TypeInformation<?> resultType, boolean deterministic)
		throws IOException {
		return new GlobalPythonScalarFunction(
			PythonBridgeUtils.getPickledGlobalPythonFunction(fullName),
			resultType,
			deterministic);
	}

	public static GlobalPythonScalarFunction create(FunctionDescriptor functionDescriptor) throws IOException {
		Map<String, String> properties = functionDescriptor.toProperties();
		assert "python".equals(properties.get("from"));
		assert properties.containsKey("full-name");
		String fullName = properties.get("full-name");
		assert properties.containsKey("return-type");
		String typeString = properties.get("return-type");
		boolean deterministic;
		if (properties.containsKey("deterministic")) {
			deterministic = Boolean.valueOf(properties.get("deterministic"));
		} else {
			deterministic = true;
		}
		TypeInformation returnType = TypeConversions.fromDataTypeToLegacyInfo(
			TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(typeString)));
		return create(fullName, returnType, deterministic);
	}
}
