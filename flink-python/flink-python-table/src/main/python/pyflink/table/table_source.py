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

from pyflink.java_gateway import ClassName
from pyflink.util.type_util import TypesUtil

__all__ = ['TableSource', 'CsvTableSource']


class TableSource(object):

    def __init__(self, j_table_source):
        self._j_table_source = j_table_source


class CsvTableSource(TableSource):

    def __init__(self, source_path, field_names, field_types):
        self._source_path = source_path
        self._field_names = field_names
        self._field_types = field_types
        j_csv_table_source = TypesUtil.class_for_name(ClassName.CSV_TABLE_SOURCE)
        j_field_names = TypesUtil.convert_py_list_to_java_array(ClassName.STRING, field_names)
        j_field_types = TypesUtil.to_java_sql_type(field_types)
        super(CsvTableSource, self).__init__(j_csv_table_source(source_path, j_field_names, j_field_types))
