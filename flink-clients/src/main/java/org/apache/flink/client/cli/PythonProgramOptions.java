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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PythonOptions;

import org.apache.commons.cli.CommandLine;

import javax.annotation.Nullable;

import static org.apache.flink.client.cli.CliFrontendParser.PYARCHIVE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYEXEC_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYFILES_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYREQUIREMENTS_OPTION;

/**
 * PythonProgramOptions.
 */
public class PythonProgramOptions {

	@Nullable
	private final String pyFiles;

	@Nullable
	private final String pyRequirements;

	@Nullable
	private final String pyExecutable;

	@Nullable
	private final String pyArchives;

	public PythonProgramOptions(
			String pyFiles,
			String pyRequirements,
			String pyExecutable,
			String pyArchives) {
		this.pyFiles = pyFiles;
		this.pyRequirements = pyRequirements;
		this.pyExecutable = pyExecutable;
		this.pyArchives = pyArchives;
	}

	public void writeToConfiguration(Configuration config) {
		if (pyFiles != null) {
			config.set(PythonOptions.PYTHON_FILES, pyFiles);
		}

		if (pyRequirements != null) {
			config.set(PythonOptions.PYTHON_REQUIREMENTS, pyRequirements);
		}

		if (pyArchives != null) {
			config.set(PythonOptions.PYTHON_ARCHIVE, pyArchives);
		}

		if (pyExecutable != null) {
			config.set(PythonOptions.PYTHON_EXECUTABLE, pyExecutable);
		}
	}

	public static PythonProgramOptions parse(CommandLine commandLine) {
		final String pyFiles;
		if (commandLine.hasOption(PYFILES_OPTION.getOpt())) {
			pyFiles = commandLine.getOptionValue(PYFILES_OPTION.getOpt());
		} else {
			pyFiles = null;
		}

		final String pyRequirements;
		if (commandLine.hasOption(PYREQUIREMENTS_OPTION.getOpt())) {
			pyRequirements = commandLine.getOptionValue(PYREQUIREMENTS_OPTION.getOpt());
		} else {
			pyRequirements = null;
		}

		final String pyArhives;
		if (commandLine.hasOption(PYARCHIVE_OPTION.getOpt())) {
			pyArhives = commandLine.getOptionValue(PYARCHIVE_OPTION.getOpt());
		} else {
			pyArhives = null;
		}

		final String pyExecutable;
		if (commandLine.hasOption(PYEXEC_OPTION.getOpt())) {
			pyExecutable = commandLine.getOptionValue(PYEXEC_OPTION.getOpt());
		} else {
			pyExecutable = null;
		}

		return new PythonProgramOptions(pyFiles, pyRequirements, pyArhives, pyExecutable);
	}
}
