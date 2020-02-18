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
import sys

import importlib
import time

from pyflink.java_gateway import get_gateway


def is_blink(t_env):
    if get_gateway().jvm.Class.forName("org.apache.flink.table.api.internal.TableEnvImpl").isInstance(t_env):
        return False
    elif t_env.getPlanner().getClass().getName() == "org.apache.flink.table.planner.StreamPlanner":
        return False
    else:
        return True


class PythonFunctionFactory(object):
    """
    Used to create PythonFunction objects for Java jobs.
    """

    def getPythonFunction(self, moduleName, objectName, tableEnvironment):
        udf_wrapper = getattr(importlib.import_module(moduleName), objectName)
        return udf_wrapper._create_judf(is_blink(tableEnvironment), tableEnvironment.getConfig())

    class Java:
        implements = ["org.apache.flink.client.python.PythonFunctionFactory"]


if __name__ == '__main__':
    gateway = get_gateway()
    gateway.entry_point.put("PythonFunctionFactory", PythonFunctionFactory())
    try:
        while sys.stdin.read():
            time.sleep(100)
    finally:
        get_gateway().close()
        exit(0)
