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

package org.apache.flink.util;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PythonOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * PythonDependencyManager.
 */
public class PythonDependencyManager {

	public static final String PYTHON_FILE_PREFIX = "python_file";
	public static final String PYTHON_REQUIREMENTS_FILE_PREFIX = "python_requirements_file";
	public static final String PYTHON_REQUIREMENTS_CACHE_PREFIX = "python_requirements_cache";
	public static final String PYTHON_ARCHIVE_PREFIX = "python_archive";

	public static final ConfigOption<Map<String, String>> PYTHON_FILES =
		ConfigOptions.key("python.internal.files-key-map").mapType().defaultValue(new HashMap<>());
	public static final ConfigOption<String> PYTHON_REQUIREMENTS_FILE =
		ConfigOptions.key("python.internal.requirements-file-key").stringType().noDefaultValue();
	public static final ConfigOption<String> PYTHON_REQUIREMENTS_CACHE =
		ConfigOptions.key("python.internal.requirements-cache-key").stringType().noDefaultValue();
	public static final ConfigOption<Map<String, String>> PYTHON_ARCHIVES =
		ConfigOptions.key("python.internal.archives-key-map").mapType().defaultValue(new HashMap<>());

	private final Configuration config;
	private final List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cachedFiles;
	private final Map<String, String> pythonFiles;
	private final Map<String, String> archives;
	private int counter = -1;

	public PythonDependencyManager(
		Configuration config,
		List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cachedFiles) {
		this.config = Preconditions.checkNotNull(config);
		this.cachedFiles = cachedFiles;
		if (config.contains(PYTHON_FILES)) {
			pythonFiles = new HashMap<>(config.get(PYTHON_FILES));
		} else {
			pythonFiles = new HashMap<>();
		}
		if (config.contains(PYTHON_ARCHIVES)) {
			archives = new HashMap<>(config.get(PYTHON_ARCHIVES));
		} else {
			archives = new HashMap<>();
		}
	}

	public void addPythonFile(String filePath) {
		Preconditions.checkNotNull(filePath);
		String fileKey = generateUniqueFileKey(PYTHON_FILE_PREFIX);
		registerCachedFile(filePath, fileKey);
		pythonFiles.put(fileKey, new File(filePath).getName());
		config.set(PYTHON_FILES, pythonFiles);
	}

	public void setPythonRequirements(String requirementsFilePath) {
		setPythonRequirements(requirementsFilePath, null);
	}

	public void setPythonRequirements(String requirementsFilePath, String requirementsCachedDir) {
		Preconditions.checkNotNull(requirementsFilePath);
		removeCachedFileIfExists(config.get(PYTHON_REQUIREMENTS_FILE));
		removeCachedFileIfExists(config.get(PYTHON_REQUIREMENTS_CACHE));
		config.removeConfig(PYTHON_REQUIREMENTS_FILE);
		config.removeConfig(PYTHON_REQUIREMENTS_CACHE);

		String fileKey = generateUniqueFileKey(PYTHON_REQUIREMENTS_FILE_PREFIX);
		registerCachedFile(requirementsFilePath, fileKey);
		config.set(PYTHON_REQUIREMENTS_FILE, fileKey);

		if (requirementsCachedDir != null) {
			String cacheDirKey = generateUniqueFileKey(PYTHON_REQUIREMENTS_CACHE_PREFIX);
			registerCachedFile(requirementsCachedDir, cacheDirKey);
			config.set(PYTHON_REQUIREMENTS_CACHE, cacheDirKey);
		}
	}

	public void addPythonArchive(String archivePath) {
		addPythonArchive(archivePath, null);
	}

	public void addPythonArchive(String archivePath, String targetDir) {
		Preconditions.checkNotNull(archivePath);

		String fileKey = generateUniqueFileKey(PYTHON_ARCHIVE_PREFIX);
		registerCachedFile(archivePath, fileKey);
		if (targetDir != null) {
			archives.put(fileKey, targetDir);
		} else {
			archives.put(fileKey, new File(archivePath).getName());
		}
		config.set(PYTHON_ARCHIVES, archives);
	}

	public void configure(ReadableConfig config) {
		config.getOptional(PythonOptions.PYTHON_FILES).ifPresent(pyFiles -> {
			for (String filePath : pyFiles.split(",")) {
				if (!fileAlreadyRegistered(filePath)) {
					addPythonFile(filePath);
				}
			}
		});

		config.getOptional(PythonOptions.PYTHON_REQUIREMENTS).ifPresent(pyRequirements -> {
			String[] requirementFileAndCache = pyRequirements.split("#", 2);
			if (requirementFileAndCache.length == 2) {
				setPythonRequirements(requirementFileAndCache[0], requirementFileAndCache[1]);
			} else {
				setPythonRequirements(requirementFileAndCache[0]);
			}
		});

		config.getOptional(PythonOptions.PYTHON_ARCHIVE).ifPresent(pyArchives -> {
			for (String archive : pyArchives.split(",")) {
				String[] filePathAndTargetDir = archive.split("#", 2);
				if (!archiveAlreadyRegistered(filePathAndTargetDir)) {
					if (filePathAndTargetDir.length == 2) {
						addPythonArchive(filePathAndTargetDir[0], filePathAndTargetDir[1]);
					} else {
						addPythonArchive(filePathAndTargetDir[0]);
					}
				}
			}
		});
	}

	private String generateUniqueFileKey(String configKey) {
		counter += 1;
		return String.format("%s_%d_%s", configKey, counter, UUID.randomUUID());
	}

	private List<String> getRegisteredFile(String keyPrefix, String path) {
		List<String> result = new ArrayList<>();
		for (Tuple2<String, DistributedCache.DistributedCacheEntry> t : cachedFiles) {
			if (t.f0.startsWith(keyPrefix)) {
				if (t.f1.filePath.equals(path)) {
					result.add(t.f0);
				}
			}
		}
		return result;
	}

	private boolean fileAlreadyRegistered(String path) {
		return !getRegisteredFile(PYTHON_FILE_PREFIX, path).isEmpty();
	}

	private boolean archiveAlreadyRegistered(String[] filePathAndTargetDir) {
		List<String> registeredFileKey = getRegisteredFile(PYTHON_ARCHIVE_PREFIX, filePathAndTargetDir[0]);
		if (!registeredFileKey.isEmpty()) {
			String targetDir;
			if (filePathAndTargetDir.length == 2) {
				targetDir = filePathAndTargetDir[1];
			} else {
				targetDir = new File(filePathAndTargetDir[0]).getName();
			}
			for (String fileKey : registeredFileKey) {
				if (targetDir.equals(archives.get(fileKey))) {
					// current archive is registered, skip it.
					return true;
				}
			}
		}
		return false;
	}

	private void removeCachedFileIfExists(String fileKey) {
		for (Tuple2<String, DistributedCache.DistributedCacheEntry> t : cachedFiles) {
			if (fileKey.equals(t.f0)) {
				cachedFiles.remove(t);
				break;
			}
		}
	}

	private void registerCachedFile(String filePath, String fileKey) {
		cachedFiles.add(new Tuple2<>(fileKey, new DistributedCache.DistributedCacheEntry(filePath, false)));
	}
}
