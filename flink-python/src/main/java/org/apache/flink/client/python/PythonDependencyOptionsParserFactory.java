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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.client.python.PythonDriverOptionsParserFactory.PYARCHIVE_OPTION;
import static org.apache.flink.client.python.PythonDriverOptionsParserFactory.PYEXEC_OPTION;
import static org.apache.flink.client.python.PythonDriverOptionsParserFactory.PYFILES_OPTION;
import static org.apache.flink.client.python.PythonDriverOptionsParserFactory.PYREQUIREMENTS_OPTION;

/**
 * PythonDependencyOptionsParserFactory.
 */
public class PythonDependencyOptionsParserFactory implements ParserResultFactory<PythonDependencyOptions> {

	public static Options getOptionList() {
		final Options options = new Options();
		options.addOption(PYFILES_OPTION);
		options.addOption(PYREQUIREMENTS_OPTION);
		options.addOption(PYARCHIVE_OPTION);
		options.addOption(PYEXEC_OPTION);
		return options;
	}

	@Override
	public Options getOptions() {
		return getOptionList();
	}

	@Override
	public PythonDependencyOptions createResult(@Nonnull CommandLine commandLine) {
		List<String> pyFiles = new ArrayList<>();

		if (commandLine.hasOption(PYFILES_OPTION.getOpt())) {
			ArrayList<Path> pythonFileList =
				Arrays.stream(commandLine.getOptionValue(PYFILES_OPTION.getOpt()).split(","))
					.map(Path::new)
					.collect(Collectors.toCollection(ArrayList::new));
			pythonFileList.forEach(path -> pyFiles.add(path.getPath()));
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

		return new PythonDependencyOptions(
			pyFiles,
			pyRequirements,
			pyExecutable,
			pyArchives);
	}
}
