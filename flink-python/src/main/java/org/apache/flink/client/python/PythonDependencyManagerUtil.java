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

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.PythonDependencyManager;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * PythonDependencyManagerUtil used to create PythonDependencyManager from ExecutionEnvironment.
 */
public class PythonDependencyManagerUtil {

	public static PythonDependencyManager createPythonDependencyManager(ExecutionEnvironment env) {
		try {
			Field cacheFile = ExecutionEnvironment.class.getDeclaredField("cacheFile");
			cacheFile.setAccessible(true);
			return new PythonDependencyManager(
				env.getConfiguration(),
				(List<Tuple2<String, DistributedCache.DistributedCacheEntry>>) cacheFile.get(env));
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public static PythonDependencyManager createPythonDependencyManager(StreamExecutionEnvironment env) {
		try {
			Method getConfiguration = StreamExecutionEnvironment.class.getDeclaredMethod("getConfiguration");
			getConfiguration.setAccessible(true);
			Configuration config = (Configuration) getConfiguration.invoke(env);
			return new PythonDependencyManager(config, env.getCachedFiles());
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}
}
