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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PythonOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.BatchTableEnvImpl;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.StreamPlanner;
import org.apache.flink.table.planner.delegation.PlannerBase;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import py4j.GatewayServer;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Utils for PythonFunctionFactory.
 */
public class PythonFunctionFactoryUtil {

	private static final long TIMEOUT_MILLIS = 3000;
	private static final long CHECK_INTERVAL = 100;

	private static PythonFunctionFactory pythonFunctionFactory = null;

	@VisibleForTesting
	static Thread pythonProcessShutdownHook = null;

	public static synchronized PythonFunctionFactory getPythonFunctionFactory(String python, Configuration config)
		throws IOException {
		if (pythonFunctionFactory != null) {
			return pythonFunctionFactory;
		} else {
			GatewayServer gatewayServer = PythonDriver.startGatewayServer();

			PythonDriverEnvUtils.PythonEnvironment env = new PythonDriverEnvUtils.PythonEnvironment();

			if (!Strings.isNullOrEmpty(python)) {
				env.pythonExec = python;
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
				if (config.contains(PythonOptions.PYTHON_FILES)) {
					List<Path> userFiles = Arrays.stream(config.get(PythonOptions.PYTHON_FILES).split(","))
						.map(Path::new).collect(Collectors.toList());
					PythonDriverEnvUtils.appendUserFiles(env, userFiles);
				}
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
			pythonProcessShutdownHook = new Thread(() -> {
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
			});
			Runtime.getRuntime().addShutdownHook(pythonProcessShutdownHook);
			return pythonFunctionFactory;
		}
	}

	public static Configuration getExecutionEnvironmentConfiguration(TableEnvironment environment)
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		if (environment instanceof TableEnvironmentImpl) {
			StreamExecutionEnvironment env;
			Planner planner = ((TableEnvironmentImpl) environment).getPlanner();
			if (planner instanceof PlannerBase) {
				env = ((PlannerBase) planner).getExecEnv();
			} else if (planner instanceof StreamPlanner) {
				env = ((StreamPlanner) planner).getExecutionEnvironment();
			} else {
				throw new RuntimeException(String.format("Unsupported Planner: %s", planner.getClass()));
			}
			Method getConfiguration = StreamExecutionEnvironment.class.getDeclaredMethod("getConfiguration");
			getConfiguration.setAccessible(true);
			return (Configuration) getConfiguration.invoke(env);
		} else if (environment instanceof BatchTableEnvImpl) {
			ExecutionEnvironment env = ((BatchTableEnvImpl) environment).execEnv();
			return env.getConfiguration();
		} else {
			throw new RuntimeException(String.format("Unsupported TableEnvironment: %s", environment.getClass()));
		}
	}
}
