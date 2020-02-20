package org.apache.flink.client.cli;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * PythonDependencyManager.
 */
public class PythonDependencyManager {

	public static final String PYTHON_FILE_PREFIX = "python_file";
	public static final String PYTHON_REQUIREMENTS_FILE_PREFIX = "python_requirements_file";
	public static final String PYTHON_REQUIREMENTS_CACHE_PREFIX = "python_requirements_cache";
	public static final String PYTHON_ARCHIVE_PREFIX = "python_archive";

	public static final ConfigOption<Map<String, String>> PYTHON_FILES =
		ConfigOptions.key("python.files").mapType().defaultValue(new HashMap<>());
	public static final ConfigOption<String> PYTHON_REQUIREMENTS_FILE =
		ConfigOptions.key("python.requirements-file").stringType().noDefaultValue();
	public static final ConfigOption<String> PYTHON_REQUIREMENTS_CACHE =
		ConfigOptions.key("python.requirements-cache").stringType().noDefaultValue();
	public static final ConfigOption<Map<String, String>> PYTHON_ARCHIVES =
		ConfigOptions.key("python.archives").mapType().defaultValue(new HashMap<>());
	public static final ConfigOption<String> PYTHON_EXEC = ConfigOptions.key("python.exec").stringType().noDefaultValue();

	private final Configuration config;
	private final List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cachedFiles;
	private Map<String, String> pythonFiles = new HashMap<>();
	private Map<String, String> archives = new HashMap<>();
	private int counter = -1;

	public PythonDependencyManager(
			Configuration config,
			List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cachedFiles) {
		this.config = Preconditions.checkNotNull(config);
		this.cachedFiles = cachedFiles;
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

	public void setPythonExecutable(String pythonExecutable) {
		Preconditions.checkNotNull(pythonExecutable);
		config.set(PYTHON_EXEC, pythonExecutable);
	}

	private String generateUniqueFileKey(String configKey) {
		counter += 1;
		return String.format("%s_%d_%s", configKey, counter, UUID.randomUUID());
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

	/**
	 *
	 * @return serialized CachedFiles configuration string, see PipelineOptions.CACHED_FILES.
	 */
	public List<String> serializeCachedFilesConfiguration() {
		return cachedFiles.stream().map(tuple -> String.format("name:%s,path:%s", tuple.f0, tuple.f1.filePath))
			.collect(Collectors.toList());
	}
}
