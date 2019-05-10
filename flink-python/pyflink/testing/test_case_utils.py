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
#################################################################################

import logging
import os
import shutil
import sys
import tempfile
import unittest

from py4j.java_gateway import JavaObject
from pyflink.table.table_source import CsvTableSource

from pyflink.java_gateway import get_gateway

from pyflink.find_flink_home import _find_flink_home
from pyflink.table import TableEnvironment, TableConfig

if sys.version_info[0] >= 3:
    xrange = range

if os.getenv("VERBOSE"):
    log_level = logging.DEBUG
else:
    log_level = logging.INFO
logging.basicConfig(stream=sys.stdout, level=log_level,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class PyFlinkTestCase(unittest.TestCase):
    """
    Base class for unit tests.
    """

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()

        os.environ["FLINK_TESTING"] = "1"
        _find_flink_home()

        logging.info("Using %s as FLINK_HOME...", os.environ["FLINK_HOME"])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    @classmethod
    def assert_equals(cls, actual, expected):
        if isinstance(actual, JavaObject):
            actual_py_list = cls.to_py_list(actual)
        else:
            actual_py_list = actual
        actual_py_list.sort()
        expected.sort()
        assert len(actual_py_list) == len(expected)
        assert all(x == y for x, y in zip(actual_py_list, expected))

    @classmethod
    def to_py_list(cls, actual):
        py_list = []
        for i in xrange(0, actual.length()):
            py_list.append(actual.apply(i))
        return py_list

    @classmethod
    def prepare_csv_source(cls, path, data, data_types, fields):
        if os.path.isfile(path):
            os.remove(path)
        csv_data = ""
        for item in data:
            if isinstance(item, list) or isinstance(item, tuple):
                csv_data += ",".join([str(element) for element in item]) + "\n"
            else:
                csv_data += str(item) + "\n"
        with open(path, 'w') as f:
            f.write(csv_data)
            f.close()
        return CsvTableSource(path, fields, data_types)


class PyFlinkStreamTableTestCase(PyFlinkTestCase):
    """
    Base class for stream unit tests.
    """

    def setUp(self):
        super(PyFlinkStreamTableTestCase, self).setUp()
        self.t_config = TableConfig.Builder().as_streaming_execution().set_parallelism(1).build()
        self.t_env = TableEnvironment.create(self.t_config)


class PyFlinkBatchTableTestCase(PyFlinkTestCase):
    """
    Base class for batch unit tests.
    """

    def setUp(self):
        super(PyFlinkBatchTableTestCase, self).setUp()
        self.t_config = TableConfig.Builder().as_batch_execution().set_parallelism(1).build()
        self.t_env = TableEnvironment.create(self.t_config)

    def collect(self, table):
        j_table = table._j_table
        gateway = get_gateway()
        row_result = self.t_env._j_tenv\
            .toDataSet(j_table, gateway.jvm.Class.forName("org.apache.flink.types.Row")).collect()
        string_result = [java_row.toString() for java_row in row_result]
        return string_result
