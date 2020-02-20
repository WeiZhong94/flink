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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.PythonProgramOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.util.FileUtils;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.python.util.ResourceUtil.extractBuiltInDependencies;

/**
 * The util class help to prepare Python env and run the python process.
 */
public final class PythonDriverEnvUtils {
	private static final Logger LOG = LoggerFactory.getLogger(PythonDriverEnvUtils.class);

	@VisibleForTesting
	public static final String PYFLINK_CLUSTER_PY_FILES = "_PYFLINK_CLUSTER_PY_FILES";

	@VisibleForTesting
	public static final String PYFLINK_CLUSTER_PY_REQUIREMENTS = "_PYFLINK_CLUSTER_PY_REQUIREMENTS";

	@VisibleForTesting
	public static final String PYFLINK_CLUSTER_PY_EXECUTABLE = "_PYFLINK_CLUSTER_PY_EXECUTABLE";

	@VisibleForTesting
	public static final String PYFLINK_CLUSTER_PY_ARCHIVES = "_PYFLINK_CLUSTER_PY_ARCHIVES";

	public static final String PYFLINK_CLIENT_EXECUTABLE = "PYFLINK_EXECUTABLE";

	@VisibleForTesting
	static Configuration globalConf = GlobalConfiguration.loadConfiguration();

	@VisibleForTesting
	static Map<String, String> systemEnv = System.getenv();

	public static String loadConfiguration(
			ConfigOption<String> configOption,
			String environmentVariableKey,
			Configuration appConf) {
		if (appConf.contains(configOption)) {
			return appConf.get(configOption);
		} else if (!Strings.isNullOrEmpty(environmentVariableKey) &&
			!Strings.isNullOrEmpty(systemEnv.get(environmentVariableKey))) {
			return systemEnv.get(environmentVariableKey);
		} else {
			return globalConf.get(configOption);
		}
	}

	/**
	 * Wraps Python exec environment.
	 */
	public static class PythonEnvironment {
		public String tempDirectory;

		public String pythonExec = "python";

		public String pythonPath;

		Map<String, String> systemEnv = new HashMap<>();
	}

	/**
	 * The hook thread that delete the tmp working dir of python process after the python process shutdown.
	 */
	private static class ShutDownPythonHook extends Thread {
		private Process p;
		private String pyFileDir;

		public ShutDownPythonHook(Process p, String pyFileDir) {
			this.p = p;
			this.pyFileDir = pyFileDir;
		}

		public void run() {

			p.destroyForcibly();

			if (pyFileDir != null) {
				File pyDir = new File(pyFileDir);
				FileUtils.deleteDirectoryQuietly(pyDir);
			}
		}
	}

	/**
	 * Prepares PythonEnvironment to start python process.
	 *
	 * @param pythonProgramOptions The Python driver options.
	 * @param tmpDir The temporary directory which files will be copied to.
	 * @return PythonEnvironment the Python environment which will be executed in Python process.
	 */
	public static PythonEnvironment preparePythonEnvironment(
			PythonProgramOptions pythonProgramOptions,
			String tmpDir) throws IOException, InterruptedException {
		PythonEnvironment env = new PythonEnvironment();
		env.pythonExec = loadConfiguration(
			PythonOptions.PYTHON_CLIENT_EXECUTABLE, PYFLINK_CLIENT_EXECUTABLE, new Configuration());
		env.pythonPath = loadConfiguration(
			PythonOptions.PYTHON_CLIENT_PYTHONPATH, null, new Configuration());

		tmpDir = new File(tmpDir).getAbsolutePath();

		// 1. setup temporary local directory for the user files
		Path tmpDirPath = new Path(tmpDir);
		FileSystem fs = tmpDirPath.getFileSystem();
		fs.mkdirs(tmpDirPath);

		env.tempDirectory = tmpDir;

		// 2. append the internal lib files to PYTHONPATH.
		appendInternalLibFiles(env);

		List<String> pythonPathList = new ArrayList<>();

		// 3. copy relevant python files to tmp dir and set them in PYTHONPATH.
		for (Path pythonFile : pythonProgramOptions.getPythonLibFiles()) {
			String sourceFileName = pythonFile.getName();
			// add random UUID parent directory to avoid name conflict.
			Path targetPath = new Path(
				tmpDirPath,
				String.join(File.separator, UUID.randomUUID().toString(), sourceFileName));
			if (!pythonFile.getFileSystem().isDistributedFS()) {
				// if the path is local file, try to create symbolic link.
				new File(targetPath.getParent().toString()).mkdir();
				createSymbolicLinkForPyflinkLib(
					Paths.get(new File(pythonFile.getPath()).getAbsolutePath()),
					Paths.get(targetPath.toString()));
			} else {
				FileUtils.copy(pythonFile, targetPath, true);
			}
			if (Files.isRegularFile(Paths.get(targetPath.toString()).toRealPath()) && sourceFileName.endsWith(".py")) {
				// add the parent directory of .py file itself to PYTHONPATH
				pythonPathList.add(targetPath.getParent().toString());
			} else {
				pythonPathList.add(targetPath.toString());
			}
		}

		if (env.pythonPath != null && !env.pythonPath.isEmpty()) {
			pythonPathList.add(env.pythonPath);
		}
		env.pythonPath = String.join(File.pathSeparator, pythonPathList);

		if (!pythonProgramOptions.getPyFiles().isEmpty()) {
			env.systemEnv.put(PYFLINK_CLUSTER_PY_FILES, String.join("\n", pythonProgramOptions.getPyFiles()));
		}
		if (!pythonProgramOptions.getPyArchives().isEmpty()) {
			env.systemEnv.put(
				PYFLINK_CLUSTER_PY_ARCHIVES,
				joinTuples(pythonProgramOptions.getPyArchives()));
		}
		pythonProgramOptions.getPyRequirements().ifPresent(
			pyRequirements -> env.systemEnv.put(
				PYFLINK_CLUSTER_PY_REQUIREMENTS,
				joinTuples(Collections.singleton(pyRequirements))));
		pythonProgramOptions.getPyExecutable().ifPresent(
			pyExecutable -> env.systemEnv.put(PYFLINK_CLUSTER_PY_EXECUTABLE, pythonProgramOptions.getPyExecutable().get()));
		return env;
	}

	public static void appendInternalLibFiles(PythonEnvironment env)
		throws IOException, InterruptedException {

		List<File> internalLibs = extractBuiltInDependencies(
			env.tempDirectory,
			UUID.randomUUID().toString(),
			true);

		List<String> pythonPathList = new ArrayList<>();

		if (env.pythonPath != null && !env.pythonPath.isEmpty()) {
			pythonPathList.add(env.pythonPath);
		}

		for (File file: internalLibs) {
			pythonPathList.add(file.getAbsolutePath());
			file.deleteOnExit();
		}

		env.pythonPath = String.join(File.pathSeparator, pythonPathList);
	}

	private static String joinTuples(Collection<Tuple2<String, String>> tuples) {
		List<String> joinedTuples = new ArrayList<>();
		for (Tuple2<String, String> tuple : tuples) {
			String f0 = tuple.f0 == null ? "" : tuple.f0;
			String f1 = tuple.f1 == null ? "" : tuple.f1;

			joinedTuples.add(String.join("\n", f0, f1));
		}
		return String.join("\n", joinedTuples);
	}

	/**
	 * Creates symbolLink in working directory for pyflink lib.
	 *
	 * @param libPath          the pyflink lib file path.
	 * @param symbolicLinkPath the symbolic link to pyflink lib.
	 */
	public static void createSymbolicLinkForPyflinkLib(java.nio.file.Path libPath, java.nio.file.Path symbolicLinkPath)
			throws IOException {
		try {
			Files.createSymbolicLink(symbolicLinkPath, libPath);
		} catch (IOException e) {
			LOG.error("Create symbol link for pyflink lib failed.", e);
			LOG.info("Try to copy pyflink lib to working directory");
			Files.copy(libPath, symbolicLinkPath);
		}
	}

	/**
	 * Starts python process.
	 *
	 * @param pythonEnv the python Environment which will be in a process.
	 * @param commands  the commands that python process will execute.
	 * @return the process represent the python process.
	 * @throws IOException Thrown if an error occurred when python process start.
	 */
	public static Process startPythonProcess(PythonEnvironment pythonEnv, List<String> commands) throws IOException {
		ProcessBuilder pythonProcessBuilder = new ProcessBuilder();
		Map<String, String> env = pythonProcessBuilder.environment();
		// combine with system PYTHONPATH
		String pythonPath = System.getenv("PYTHONPATH");
		if (!Strings.isNullOrEmpty(pythonPath)) {
			pythonPath = String.join(File.pathSeparator, pythonEnv.pythonPath, pythonPath);
		} else {
			pythonPath = pythonEnv.pythonPath;
		}
		env.put("PYTHONPATH", pythonEnv.pythonPath);
		pythonEnv.systemEnv.forEach(env::put);
		commands.add(0, pythonEnv.pythonExec);
		pythonProcessBuilder.command(commands);
		// redirect the stderr to stdout
		pythonProcessBuilder.redirectErrorStream(true);
		// set the child process the output same as the parent process.
		pythonProcessBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
		Process process = pythonProcessBuilder.start();
		if (!process.isAlive()) {
			throw new RuntimeException("Failed to start Python process. ");
		}

		// Make sure that the python sub process will be killed when JVM exit
		ShutDownPythonHook hook = new ShutDownPythonHook(process, pythonEnv.tempDirectory);
		Runtime.getRuntime().addShutdownHook(hook);

		return process;
	}
}
