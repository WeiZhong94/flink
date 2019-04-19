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
from threading import RLock
from typing import Union

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
                TYPES = TypesUtil.class_for_name(ClassName.TYPES)
                TypesUtil._sql_basic_types_py2j_map = {
                    DataTypes.STRING: TYPES.STRING,
                    DataTypes.BOOLEAN: TYPES.BOOLEAN,
                    DataTypes.BYTE: TYPES.BYTE,
                    DataTypes.CHAR: TYPES.CHAR,
                    DataTypes.INT: TYPES.INT,
                    DataTypes.LONG: TYPES.LONG,
                    DataTypes.FLOAT: TYPES.FLOAT,
                    DataTypes.SHORT: TYPES.SHORT,
                    DataTypes.DOUBLE: TYPES.DOUBLE,
                    DataTypes.DATE: TYPES.SQL_DATE,
                    DataTypes.TIME: TYPES.SQL_TIME,
                    DataTypes.TIMESTAMP: TYPES.SQL_TIMESTAMP,
                    DataTypes.BYTE_ARRARY: TYPES.PRIMITIVE_ARRAY(TYPES.BYTE),
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
