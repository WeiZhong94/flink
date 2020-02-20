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
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.client.cli.CliFrontendParser.PYARCHIVE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYEXEC_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYFILES_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYMODULE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYREQUIREMENTS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PY_OPTION;

/**
 * Parser factory which generates a {@link PythonProgramOptions} from a given
 * list of command line arguments.
 */
public final class PythonProgramOptionsParserFactory implements ParserResultFactory<PythonProgramOptions> {

	@Override
	public Options getOptions() {
		final Options options = new Options();
		options.addOption(PY_OPTION);
		options.addOption(PYMODULE_OPTION);
		options.addOption(PYFILES_OPTION);
		options.addOption(PYREQUIREMENTS_OPTION);
		options.addOption(PYARCHIVE_OPTION);
		options.addOption(PYEXEC_OPTION);
		return options;
	}

	@Override
	public PythonProgramOptions createResult(@Nonnull CommandLine commandLine) throws FlinkParseException {
		String entrypointModule = null;
		final List<Path> pythonLibFiles = new ArrayList<>();

		if (commandLine.hasOption(PY_OPTION.getOpt()) && commandLine.hasOption(PYMODULE_OPTION.getOpt())) {
			throw new FlinkParseException("Cannot use options -py and -pym simultaneously.");
		} else if (commandLine.hasOption(PY_OPTION.getOpt())) {
			Path file = new Path(commandLine.getOptionValue(PY_OPTION.getOpt()));
			String fileName = file.getName();
			if (fileName.endsWith(".py")) {
				entrypointModule = fileName.substring(0, fileName.length() - 3);
				pythonLibFiles.add(file);
			}
		} else if (commandLine.hasOption(PYMODULE_OPTION.getOpt())) {
			entrypointModule = commandLine.getOptionValue(PYMODULE_OPTION.getOpt());
		} else {
			throw new FlinkParseException(
				"The Python entrypoint has not been specified. It can be specified with options -py or -pym");
		}

		List<String> pyFiles = new ArrayList<>();

		if (commandLine.hasOption(PYFILES_OPTION.getOpt())) {
			ArrayList<Path> pythonFileList =
				Arrays.stream(commandLine.getOptionValue(PYFILES_OPTION.getOpt()).split(","))
				.map(Path::new)
				.collect(Collectors.toCollection(ArrayList::new));
			pythonFileList.forEach(path -> pyFiles.add(path.getPath()));
			pythonLibFiles.addAll(pythonFileList);
		} else if (commandLine.hasOption(PYMODULE_OPTION.getOpt())) {
			throw new FlinkParseException("Option -pym must be used in conjunction with `--pyFiles`");
		}

		Tuple2<String, String> pyRequirements = null;
		if (commandLine.hasOption(PYREQUIREMENTS_OPTION.getOpt())) {
			String[] optionValues = commandLine.getOptionValue(PYREQUIREMENTS_OPTION.getOpt()).split("#");
			pyRequirements = new Tuple2<>();
			pyRequirements.f0 = optionValues[0];
			if (optionValues.length > 1) {
				pyRequirements.f1 = optionValues[1];
			}
		}

		List<Tuple2<String, String>> pyArchives = new ArrayList<>();
		if (commandLine.hasOption(PYARCHIVE_OPTION.getOpt())) {
			String[] archiveEntries = commandLine.getOptionValue(PYARCHIVE_OPTION.getOpt()).split(",");
			for (String archiveEntry : archiveEntries) {
				String[] pathAndTargetDir = archiveEntry.split("#");
				if (pathAndTargetDir.length > 1) {
					pyArchives.add(new Tuple2<>(pathAndTargetDir[0], pathAndTargetDir[1]));
				} else {
					pyArchives.add(new Tuple2<>(pathAndTargetDir[0], null));
				}
			}
		}

		String pyExecutable = null;
		if (commandLine.hasOption(PYEXEC_OPTION.getOpt())) {
			pyExecutable = commandLine.getOptionValue(PYEXEC_OPTION.getOpt());
		}

		return new PythonProgramOptions(
			entrypointModule,
			pythonLibFiles,
			commandLine.getArgList(),
			pyFiles,
			pyRequirements,
			pyExecutable,
			pyArchives);
	}
}
