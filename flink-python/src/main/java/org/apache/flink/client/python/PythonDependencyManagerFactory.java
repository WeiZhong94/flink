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

package org.apache.flink.client.python;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.PythonDependencyManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * PythonDependencyManagerFactory.
 */
public class PythonDependencyManagerFactory {

	public static PythonDependencyManager create(ExecutionEnvironment env) {
		Configuration config = env.getConfiguration();
		List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cachedFiles;
		try {
			Field field = ExecutionEnvironment.class.getDeclaredField("cacheFile");
			field.setAccessible(true);
			cachedFiles = (List<Tuple2<String, DistributedCache.DistributedCacheEntry>>) field.get(env);
		} catch (IllegalAccessException | NoSuchFieldException e) {
			throw new IllegalArgumentException("Can not get the cacahe file list from given environment.", e);
		}
		return new PythonDependencyManager(config, cachedFiles);
	}

	public static PythonDependencyManager create(StreamExecutionEnvironment env) {
		List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cachedFiles = env.getCachedFiles();
		Configuration config;
		try {
			Method method = StreamExecutionEnvironment.class.getDeclaredMethod("getConfiguration");
			method.setAccessible(true);
			config = (Configuration) method.invoke(env);
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			throw new IllegalArgumentException("Can not get the configuration from given environment.", e);
		}
		return new PythonDependencyManager(config, cachedFiles);
	}
}
