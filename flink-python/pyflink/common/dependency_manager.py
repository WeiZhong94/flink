################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from pyflink.java_gateway import get_gateway

__all__ = ['DependencyManager']


class DependencyManager(object):
    """
    Utility class for dependency management. The dependencies will be registered at the distributed
    cache.
    """

    def __init__(self, j_env):
        gateway = get_gateway()
        self._j_dependency_manager = \
            gateway.jvm.org.apache.flink.client.python.PythonDependencyManagerFactory.create(j_env)

    def add_python_file(self, file_path):
        self._j_dependency_manager.addPythonFile(file_path)

    def set_python_requirements(self, requirements_file_path, requirements_cached_dir=None):
        self._j_dependency_manager.setPythonRequirements(
            requirements_file_path, requirements_cached_dir)

    def add_python_archive(self, archive_path, target_dir=None):
        self._j_dependency_manager.addPythonArchive(archive_path, target_dir)
