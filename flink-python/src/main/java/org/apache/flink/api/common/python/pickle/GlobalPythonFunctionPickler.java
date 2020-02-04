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

package org.apache.flink.api.common.python.pickle;

import net.razorvine.pickle.IObjectPickler;
import net.razorvine.pickle.Opcodes;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Pickler of GlobalPythonFunction.
 */
public class GlobalPythonFunctionPickler implements IObjectPickler {
	@Override
	public void pickle(Object o, OutputStream out, Pickler currentPickler) throws PickleException, IOException {
		GlobalPythonFunction globalPythonFunction = (GlobalPythonFunction) o;
		out.write(Opcodes.GLOBAL);
		out.write("pyflink.table.udf\nDelegatingScalarFunction\n".getBytes());
		out.write(Opcodes.EMPTY_TUPLE);
		out.write(Opcodes.NEWOBJ);
		out.write(Opcodes.EMPTY_DICT);
		out.write(Opcodes.BINUNICODE);
		String paramName = "func";
		out.write(intToBytesLittleEndian(paramName.length()));
		out.write(paramName.getBytes());
		out.write(Opcodes.GLOBAL);
		out.write(globalPythonFunction.getModuleName().getBytes());
		out.write("\n".getBytes());
		out.write(globalPythonFunction.getFunctionName().getBytes());
		out.write("\n".getBytes());
		out.write(Opcodes.SETITEM);
		out.write(Opcodes.BUILD);
	}

	private static byte[] intToBytesLittleEndian(int value) {
		byte[] src = new byte[4];
		src[0] = (byte) (value & 0xFF);
		src[1] = (byte) ((value >> 8) & 0xFF);
		src[2] = (byte) ((value >> 16) & 0xFF);
		src[3] = (byte) ((value >> 24) & 0xFF);
		return src;
	}
}
