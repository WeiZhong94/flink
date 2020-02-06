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

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Dependency manager.
 */
public class DependencyManager {

	public final String PYTHON_FILE_PREFIX = "python_file";
	public final String PYTHON_REQUIREMENTS_FILE_PREFIX = "python_requirements_file";
	public final String PYTHON_REQUIREMENTS_CACHE_PREFIX = "python_requirements_cache";
	public final String PYTHON_ARCHIVE_PREFIX = "python_archive";

	public final ConfigOption<String> PYTHON_FILES = ConfigOptions.key("python.files").stringType().defaultValue("");
	public final ConfigOption<String> PYTHON_REQUIREMENTS_FILE =
		ConfigOptions.key("python.requirements-file").stringType().defaultValue("");
	public final ConfigOption<String> PYTHON_REQUIREMENTS_CACHE =
		ConfigOptions.key("python.requirements-cache").stringType().defaultValue("");
	public final ConfigOption<String> PYTHON_ARCHIVES =
		ConfigOptions.key("python.archives").stringType().defaultValue("");
	public final ConfigOption<String> PYTHON_EXEC = ConfigOptions.key("python.exec").stringType().defaultValue("");

	private final Configuration config;
	private ExecutionEnvironment env = null;
	private StreamExecutionEnvironment senv = null;
	private Map<String, String> pythonFiles = new HashMap<>();
	private Map<String, String> archives = new HashMap<>();
	private int counter = -1;
	private ObjectMapper jsonMapper = new ObjectMapper();

	public DependencyManager(Configuration config, ExecutionEnvironment env) {
		this.config = Preconditions.checkNotNull(config);
		this.env = Preconditions.checkNotNull(env);
	}

	public DependencyManager(Configuration config, StreamExecutionEnvironment senv) {
		this.config = Preconditions.checkNotNull(config);
		this.senv = Preconditions.checkNotNull(senv);
	}

	public void addPythonFile(String filePath) throws JsonProcessingException {
		Preconditions.checkNotNull(filePath);
		String fileKey = generateUniqueFileKey(PYTHON_FILE_PREFIX);
		registerCachedFile(filePath, fileKey);
		pythonFiles.put(fileKey, new File(filePath).getName());
		config.set(PYTHON_FILES, jsonMapper.writeValueAsString(pythonFiles));
	}

	public void setPythonRequirements(String requirementsFilePath) {
		setPythonRequirements(requirementsFilePath, null);
	}

	public void setPythonRequirements(String requirementsFilePath, String requirementsCachedDir) {
		Preconditions.checkNotNull(requirementsFilePath);
		removeCachedFileIfExists(PYTHON_REQUIREMENTS_FILE);
		removeCachedFileIfExists(PYTHON_REQUIREMENTS_CACHE);

		String fileKey = generateUniqueFileKey(PYTHON_REQUIREMENTS_FILE_PREFIX);
		registerCachedFile(requirementsFilePath, fileKey);
		config.set(PYTHON_REQUIREMENTS_FILE, fileKey);

		if (requirementsCachedDir != null) {
			String cacheDirKey = generateUniqueFileKey(PYTHON_REQUIREMENTS_CACHE_PREFIX);
			registerCachedFile(requirementsCachedDir, cacheDirKey);
			config.set(PYTHON_REQUIREMENTS_CACHE, cacheDirKey);
		}
	}

	public void addPythonArchive(String archivePath) throws JsonProcessingException {
		addPythonArchive(archivePath, null);
	}

	public void addPythonArchive(String archivePath, String targetDir) throws JsonProcessingException {
		Preconditions.checkNotNull(archivePath);

		String fileKey = generateUniqueFileKey(PYTHON_ARCHIVE_PREFIX);
		registerCachedFile(archivePath, fileKey);
		if (targetDir != null) {
			archives.put(fileKey, targetDir);
		} else {
			archives.put(fileKey, new File(archivePath).getName());
		}
		config.set(PYTHON_ARCHIVES, jsonMapper.writeValueAsString(archives));
	}

	public void setPythonExecutable(String pythonExecutable) {
		Preconditions.checkNotNull(pythonExecutable);
		config.set(PYTHON_EXEC, pythonExecutable);
	}

	private String generateUniqueFileKey(String configKey) {
		counter += 1;
		return String.format("%s_%d_%s", configKey, counter, UUID.randomUUID());
	}

	private void removeCachedFileIfExists(ConfigOption<String> configOption) {
		if (config.contains(configOption)) {
			String fileKey = config.getString(configOption);
			List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cachedFiles = getCachedFiles();
			for (Tuple2<String, DistributedCache.DistributedCacheEntry> t : cachedFiles) {
				if (fileKey.equals(t.f0)) {
					cachedFiles.remove(t);
					break;
				}
			}
			config.removeConfig(configOption);
		}
	}

	private List<Tuple2<String, DistributedCache.DistributedCacheEntry>> getCachedFiles() {
		if (env != null) {
			try {
				Field f = env.getClass().getField("cacheFile");
				f.setAccessible(true);
				return (List<Tuple2<String, DistributedCache.DistributedCacheEntry>>) f.get(env);
			} catch (NoSuchFieldException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		} else {
			assert senv != null;
			return senv.getCachedFiles();
		}
	}

	private void registerCachedFile(String filePath, String fileKey) {
		if (env != null) {
			env.registerCachedFile(filePath, fileKey);
		} else {
			assert senv != null;
			senv.registerCachedFile(filePath, fileKey);
		}
	}
}
