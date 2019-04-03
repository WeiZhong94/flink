# ###############################################################################
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

from abc import ABCMeta

from pyflink.java_gateway import ClassName
from pyflink.table import Table
from pyflink.table.table_config import TableConfig
from pyflink.util.type_util import TypesUtil

__all__ = [
    'BatchTableEnvironment',
    'StreamTableEnvironment',
    'TableEnvironment'
]


class TableEnvironment(object):
    """
    Wrapper for org.apache.flink.table.api.java.TableEnvironment
    """

    __metaclass__ = ABCMeta

    def __init__(self, j_tenv):
        self._j_tenv = j_tenv

    def from_table_source(self, table_source):
        self._j_tenv.fromTableSource(table_source.j_table_source)

    def register_table(self, name, table):
        self._j_tenv.registerTable(name, table._java_table)

    def register_table_source(self, name, table_source):
        self._j_tenv.registerTableSource(name, table_source.j_table_source)

    def register_table_sink(self, name, field_names_or_table_sink, field_types=None, table_sink=None):
        if table_sink is not None:
            # j_field_names =
            field_names = field_names_or_table_sink
            j_names = TypesUtil.convert_py_list_to_java_array(ClassName.STRING,
                                                              field_names)
            j_types = []
            for field_type in field_types:
                j_types.append(TypesUtil.to_java_sql_type(field_type))
            j_types_array = TypesUtil.convert_py_list_to_java_array(ClassName.TYPE_INFORMATION,
                                                                    j_types)

            self._j_tenv.registerTableSink(name, j_names, j_types_array, table_sink._j_table_sink)
        else:
            table_sink = field_names_or_table_sink
            self._j_tenv.registerTableSink(name, table_sink._j_table_sink)

    def scan(self, *table_path):
        j_varargs = TypesUtil.convert_py_list_to_java_array(ClassName.STRING, table_path)
        j_table = self._j_tenv.scan(j_varargs)
        return Table(j_table)

    def execute(self, job_name=None):
        if job_name is not None:
            self._j_tenv.execEnv().execute(job_name)
        else:
            self._j_tenv.execEnv().execute()

    @classmethod
    def get_table_environment(cls, table_config):
        """

        :type table_config: TableConfig
        :return:
        """
        if table_config.is_stream:
            t_env = TableEnvironment._get_stream_table_environment()
        else:
            t_env = TableEnvironment._get_batch_table_environment()

        if table_config.parallelism is not None:
            t_env._j_tenv.execEnv().setParallelism(table_config.parallelism)

        return t_env

    @classmethod
    def _get_stream_table_environment(cls):
        table_env = TypesUtil.class_for_name(ClassName.TABLE_ENVIRONMENT)
        _cls = TypesUtil.class_for_name(ClassName.STREAM_EXECUTION_ENVIRONMENT)
        j_env = _cls.getExecutionEnvironment()
        j_t_env = table_env.getTableEnvironment(j_env)
        return StreamTableEnvironment(j_t_env)

    @classmethod
    def _get_batch_table_environment(cls):
        table_env = TypesUtil.class_for_name(ClassName.TABLE_ENVIRONMENT)
        _cls = TypesUtil.class_for_name(ClassName.EXECUTION_ENVIRONMENT)
        j_env = _cls.getExecutionEnvironment()
        j_t_env = table_env.getTableEnvironment(j_env)
        return BatchTableEnvironment(j_t_env)


class StreamTableEnvironment(TableEnvironment):
    """
    Wrapper for org.apache.flink.table.api.java.StreamTableEnvironment
    """

    def __init__(self, env):
        self._j_tenv = env
        super(StreamTableEnvironment, self).__init__(env)

    def _from_data_stream(self, data_stream, fields=None):
        if fields is None:
            j_table = self._j_tenv.fromDataStream(data_stream)
        else:
            j_table = self._j_tenv.fromDataStream(data_stream, fields)
        return Table(j_table)

    def from_collection(self, data, fields=None):
        if type(data[0]) is tuple:
            java_list = TypesUtil.convert_tuple_list(data)
        else:
            java_list = TypesUtil.convert_pylist_to_java_list(data)
        j_ds_source = self._j_tenv.execEnv().fromCollection(java_list)
        return self._from_data_stream(j_ds_source, fields)


class BatchTableEnvironment(TableEnvironment):
    """
    Wrapper for org.apache.flink.table.api.java.BatchTableEnvironment
    """

    def __init__(self, env):
        self._j_tenv = env
        super(BatchTableEnvironment, self).__init__(env)

    def _from_data_set(self, data_set, fields=None):
        if fields is None:
            j_table = self._j_tenv.fromDataSet(data_set)
        else:
            j_table = self._j_tenv.fromDataSet(data_set, fields)
        return Table(j_table)

    def from_collection(self, data, fields=None):
        if type(data[0]) is tuple:
            java_list = TypesUtil.convert_tuple_list(data)
        else:
            java_list = TypesUtil.convert_pylist_to_java_list(data)
        j_ds_source = self._j_tenv.execEnv().fromCollection(java_list)
        return self._from_data_set(j_ds_source, fields)
