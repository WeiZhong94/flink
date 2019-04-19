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
from datetime import date, datetime, time as datetime_time
import time

from threading import RLock
from typing import Union

from py4j.java_collections import ListConverter
from py4j.java_gateway import JavaClass, JavaObject

from pyflink.java_gateway import get_gateway, ClassName
from pyflink.table.data_type import *
from pyflink.table.data_type import DataType

if sys.version > '3':
    xrange = range


class TypesUtil(object):
    _sql_basic_types_py2j_map = None
    _init_lock = RLock()

    @staticmethod
    def to_java_sql_type(py_sql_type):
        # type: (Union[list[DataType],DataType]) -> JavaObject
        if TypesUtil._sql_basic_types_py2j_map is None:
            with TypesUtil._init_lock:
                j_sql_types = TypesUtil.class_for_name(ClassName.TYPES)
                TypesUtil._sql_basic_types_py2j_map = {
                    DataTypes.STRING: j_sql_types.STRING,
                    DataTypes.INT: j_sql_types.INT,
                    DataTypes.BOOLEAN: j_sql_types.BOOLEAN,
                    DataTypes.DOUBLE: j_sql_types.DOUBLE,
                    DataTypes.FLOAT: j_sql_types.FLOAT,
                    DataTypes.BYTE: j_sql_types.BYTE,
                    DataTypes.LONG: j_sql_types.LONG,
                    DataTypes.SHORT: j_sql_types.SHORT,
                    DataTypes.CHAR: j_sql_types.CHAR,
                    DataTypes.BYTE_ARRARY: j_sql_types.PRIMITIVE_ARRAY(j_sql_types.BYTE),
                    DataTypes.DATE: j_sql_types.SQL_DATE,
                    DataTypes.TIME: j_sql_types.SQL_TIME,
                    DataTypes.TIMESTAMP: j_sql_types.SQL_TIMESTAMP,
                    DataTypes.ROWTIME_INDICATOR:
                        TypesUtil.class_for_name(ClassName.TIME_INDICATOR_TYPE_INFO).ROWTIME_INDICATOR(),
                    DataTypes.PROCTIME_INDICATOR:
                        TypesUtil.class_for_name(ClassName.TIME_INDICATOR_TYPE_INFO).PROCTIME_INDICATOR()
                }

        if isinstance(py_sql_type, list):
            j_types = [TypesUtil._sql_basic_types_py2j_map[pt] for pt in py_sql_type]
            j_types_arr = TypesUtil.convert_py_list_to_java_array(
                ClassName.TYPE_INFORMATION,
                j_types
            )
            return j_types_arr

        return TypesUtil._sql_basic_types_py2j_map.get(py_sql_type)

    @staticmethod
    def convert_pylist_to_java_list(py_list):
        # type: (list) -> JavaObject
        _gateway = get_gateway()
        java_list = [TypesUtil.python_value_to_java_value(item) for item in py_list]
        j_list = ListConverter().convert(
            java_list, _gateway._gateway_client)
        return j_list

    @staticmethod
    def convert_tuple_list(tuple_list):
        # type: (list[tuple]) -> JavaObject
        _gateway = get_gateway()
        java_tuple_list = [TypesUtil._tuple_to_java_tuple(item) for item in tuple_list]
        return ListConverter().convert(
            java_tuple_list, _gateway._gateway_client)

    @staticmethod
    def _tuple_to_java_tuple(data):
        # type: (tuple) -> JavaObject
        size = len(data)
        java_data = [TypesUtil.python_value_to_java_value(item) for item in data]
        java_tuple = TypesUtil.class_for_name(ClassName.TUPLE + str(size))
        return java_tuple(*list(java_data))

    @staticmethod
    def convert_py_list_to_java_array(arr_type, seq):
        # type: (str, Union[list,tuple]) -> JavaObject
        _gateway = get_gateway()
        ns = TypesUtil.class_for_name(arr_type)
        size = len(seq)
        java_array = _gateway.new_array(ns, size)
        for i in range(size):
            java_array[i] = seq[i]

        return java_array

    @staticmethod
    def class_for_name(class_name):
        # type: (str) -> JavaClass
        _gateway = get_gateway()
        clz = getattr(_gateway.jvm, class_name)
        if not isinstance(clz, JavaClass):
            # a workaround to access nested class
            attrs = class_name.split('.')
            for i in reversed(xrange(len(attrs) - 1)):
                outer_class_name = ''
                for j in xrange(i + 1):
                    # outer class canonical name
                    if len(outer_class_name) > 0:
                        outer_class_name += '.'
                    outer_class_name += attrs[j]
                internal_class_name = ''
                for j in xrange(len(attrs) - 1 - i):
                    if len(internal_class_name) > 0:
                        internal_class_name += '.'
                    internal_class_name += attrs[i + j + 1]
                clz = getattr(_gateway.jvm, outer_class_name)
                clz = getattr(clz, internal_class_name)
                if isinstance(clz, JavaClass):
                    return clz
        return clz

    @staticmethod
    def python_value_to_java_value(value):
        if isinstance(value, datetime):
            if sys.version >= 3:
                ts = int(value.timestamp() * 1000)
            else:
                mills = value.microsecond // 1000
                ts = int(time.mktime(value.timetuple()) * 1000 + mills)
            clz = TypesUtil.class_for_name(ClassName.TIMESTAMP)
            return clz(ts)
        if isinstance(value, date):
            clz = TypesUtil.class_for_name(ClassName.DATE)
            return clz.valueOf(value.strftime("%Y-%m-%d"))
        if isinstance(value, datetime_time):
            clz = TypesUtil.class_for_name(ClassName.TIME)
            return clz.valueOf(value.strftime("%H:%M:%S"))
        return value
