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

from py4j.java_gateway import JavaClass, JavaObject
from pyflink.java_gateway import get_gateway, ClassName
from pyflink.table.types import DataTypes

if sys.version > '3':
    xrange = range

_data_types_mapping = None
_init_lock = RLock()


def to_java_type(py_type):
    global _data_types_mapping
    global _init_lock

    if _data_types_mapping is None:
        with _init_lock:
            TYPES = class_for_name(ClassName.TYPES)
            _data_types_mapping = {
                DataTypes.STRING: TYPES.STRING,
                DataTypes.BOOLEAN: TYPES.BOOLEAN,
                DataTypes.BYTE: TYPES.BYTE,
                DataTypes.CHAR: TYPES.CHAR,
                DataTypes.SHORT: TYPES.SHORT,
                DataTypes.INT: TYPES.INT,
                DataTypes.LONG: TYPES.LONG,
                DataTypes.FLOAT: TYPES.FLOAT,
                DataTypes.DOUBLE: TYPES.DOUBLE,
                DataTypes.DATE: TYPES.SQL_DATE,
                DataTypes.TIME: TYPES.SQL_TIME,
                DataTypes.TIMESTAMP: TYPES.SQL_TIMESTAMP
            }

    return _data_types_mapping[py_type]


def convert_py_list_to_java_array(arr_type, seq):
    # type: (str, Union[list,tuple]) -> JavaObject
    gateway = get_gateway()
    size = len(seq)
    java_array = gateway.new_array(class_for_name(arr_type), size)
    for i in range(size):
        java_array[i] = seq[i]

    return java_array


def class_for_name(class_name):
    # type: (str) -> JavaClass
    gateway = get_gateway()
    clz = getattr(gateway.jvm, class_name)
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
            clz = getattr(gateway.jvm, outer_class_name)
            clz = getattr(clz, internal_class_name)
            if isinstance(clz, JavaClass):
                return clz
    return clz
