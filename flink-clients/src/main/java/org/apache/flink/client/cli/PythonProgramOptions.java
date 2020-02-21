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

package org.apache.flink.client.cli;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Options for the PythonDriver.
 */
public class PythonProgramOptions {

	@Nullable
	private String entrypointModule;

	@Nonnull
	private List<Path> pythonLibFiles;

	@Nonnull
	private List<String> programArgs;

	@Nonnull
	private final List<String> pyFiles;

	@Nullable
	private final Tuple2<String, String> pyRequirements;

	@Nullable
	private final String pyExecutable;

	@Nonnull
	private final List<Tuple2<String, String>> pyArchives;

	public Optional<String> getEntrypointModule() {
		return Optional.ofNullable(entrypointModule);
	}

	@Nonnull
	public List<Path> getPythonLibFiles() {
		return pythonLibFiles;
	}

	@Nonnull
	public List<String> getProgramArgs() {
		return programArgs;
	}

	@Nonnull
	public List<String> getPyFiles() {
		return pyFiles;
	}

	public Optional<Tuple2<String, String>> getPyRequirements() {
		return Optional.ofNullable(pyRequirements);
	}

	public Optional<String> getPyExecutable() {
		return Optional.ofNullable(pyExecutable);
	}

	@Nonnull
	public List<Tuple2<String, String>> getPyArchives() {
		return pyArchives;
	}

	public PythonProgramOptions(
			String entrypointModule,
			List<Path> pythonLibFiles,
			List<String> programArgs,
			List<String> pyFiles,
			Tuple2<String, String> pyRequirements,
			String pyExecutable,
			List<Tuple2<String, String>> pyArchives) {
		this.entrypointModule = entrypointModule;
		this.pythonLibFiles = requireNonNull(pythonLibFiles, "pythonLibFiles");
		this.programArgs = requireNonNull(programArgs, "programArgs");
		this.pyFiles = requireNonNull(pyFiles, "pyFiles");
		this.pyRequirements = pyRequirements;
		this.pyExecutable = pyExecutable;
		this.pyArchives = requireNonNull(pyArchives, "pyArchives");
	}
}
