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

__all__ = ['TableSink', 'CsvTableSink']


class TableSink(object):
    """
    A :class:`TableSink` specifies how to emit a table to an external system or location.
    """

    def __init__(self, j_table_sink):
        self._j_table_sink = j_table_sink


class WriteMode(object):
    NO_OVERWRITE = 0
    OVERWRITE = 1


class CsvTableSink(TableSink):
    """
    A simple :class:`TableSink` to emit data as CSV files.

    :param path: The output path to write the Table to.
    :param field_delimiter: The field delimiter.
    :param num_files: The number of files to write to.
    :param write_mode: The write mode to specify whether existing files are overwritten or not.
    """

    def __init__(self, path, field_delimiter=',', num_files=1, write_mode=None):
        # type: (str, str, int, int) -> None
        if write_mode is None:
            self._write_mode = TypesUtil.class_for_name(ClassName.WRITE_MODE).NO_OVERWRITE
        else:
            self._write_mode = TypesUtil.class_for_name(ClassName.WRITE_MODE).OVERWRITE
        csv_table_sink = TypesUtil.class_for_name(ClassName.CSV_TABLE_SINK)
        j_csv_table_sink = csv_table_sink(path, field_delimiter, num_files, self._write_mode)
        super(CsvTableSink, self).__init__(j_csv_table_sink)
