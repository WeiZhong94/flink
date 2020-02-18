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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import py4j.GatewayServer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * PythonFunctionFactoryUtil.
 */
public class PythonFunctionFactoryUtil {

	private static final long TIMEOUT_MILLIS = 3000;
	private static final long CHECK_INTERVAL = 100;

	private static PythonFunctionFactory pythonFunctionFactory = null;

	public static synchronized PythonFunctionFactory getPythonFunctionFactory(String python, String pythonPath)
		throws IOException {
		Preconditions.checkNotNull(python);
		Preconditions.checkNotNull(pythonPath);

		if (pythonFunctionFactory != null) {
			return pythonFunctionFactory;
		} else {
			GatewayServer gatewayServer = PythonDriver.startGatewayServer();

			PythonDriverEnvUtils.PythonEnvironment env = new PythonDriverEnvUtils.PythonEnvironment();

			if (!python.isEmpty()) {
				env.pythonExec = python;
			}
			if (!pythonPath.isEmpty()) {
				env.pythonPath = pythonPath;
			}

			// prepare the exec environment of python progress.
			String tmpDir = System.getProperty("java.io.tmpdir") +
				File.separator + "pyflink" + File.separator + UUID.randomUUID();
			Path tmpDirPath = new Path(tmpDir);
			FileSystem fs = tmpDirPath.getFileSystem();
			fs.mkdirs(tmpDirPath);
			env.tempDirectory = tmpDir;

			try {
				PythonDriverEnvUtils.appendInternalLibFiles(env);
			} catch (InterruptedException e) {
				throw new RuntimeException("Prepare python process environment failed!", e);
			}
			env.systemEnv.put(
				"PYFLINK_GATEWAY_PORT",
				String.valueOf(gatewayServer.getListeningPort()));

			List<String> commands = new ArrayList<>();
			commands.add("-m");
			commands.add("pyflink.java_rpc_server");
			Process pythonProcess;
			pythonProcess = PythonDriverEnvUtils.startPythonProcess(env, commands);
			Map<String, Object> entryPoint = (Map<String, Object>) gatewayServer.getGateway().getEntryPoint();
			int i = 0;
			while (!entryPoint.containsKey("PythonFunctionFactory")) {
				if (!pythonProcess.isAlive()) {
					throw new RuntimeException("Python process environment start failed!");
				}
				try {
					Thread.sleep(CHECK_INTERVAL);
				} catch (InterruptedException e) {
					throw new RuntimeException("Interrupted when waiting python process start.", e);
				}
				i++;
				if (i > TIMEOUT_MILLIS / CHECK_INTERVAL) {
					throw new RuntimeException("Python process environment start failed!");
				}
			}
			pythonFunctionFactory = (PythonFunctionFactory) entryPoint.get("PythonFunctionFactory");
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				gatewayServer.shutdown();
				pythonProcess.destroy();
				try {
					pythonProcess.waitFor(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					throw new RuntimeException("Interrupted.", e);
				}
				if (pythonProcess.isAlive()) {
					pythonProcess.destroyForcibly();
				}
			}));
			return pythonFunctionFactory;
		}
	}

}
