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

if sys.version > '3':
    xrange = range

__all__ = [
    'StringType',
    'BooleanType',
    'ShortType',
    'ByteType',
    'CharType',
    'IntegerType',
    'LongType',
    'FloatType',
    'DoubleType',
    'BinaryType',
    'DateType',
    'TimeType',
    'TimestampType',
    'DataTypes',
]


class DataType(object):
    """ data types.
    """
    @classmethod
    def type_name(cls):
        return cls.__name__[:-4].lower()

    def __hash__(self):
        return hash(self.type_name())

    def __eq__(self, other):
        return self.type_name() == other.type_name()

    def __ne__(self, other):
        return self.type_name() != other.type_name()


class StringType(DataType):
    """String data type.  SQL VARCHAR
    """


class BooleanType(DataType):
    """Boolean data types. SQL BOOLEAN
    """


class ShortType(DataType):
    """Short data types.  SQL SMALLINT (16bits)
    """


class ByteType(DataType):
    """Byte data type. SQL TINYINT
    """


class CharType(DataType):
    """
    Char data type. SQL CHAR
    """


class IntegerType(DataType):
    """Int data types. SQL INT (32bits)
    """


class LongType(DataType):
    """Long data types. SQL BIGINT (64bits)
    """


class FloatType(DataType):
    """Float data type. SQL FLOAT
    """


class DoubleType(DataType):
    """Double data type. SQL DOUBLE
    """


class BinaryType(DataType):
    """Bytes data type.  SQL VARBINARY
    """


class DateType(DataType):
    """Date data type.  SQL DATE
    """


class TimeType(DataType):
    """Time data type. SQL TIME
    """


class TimestampType(DataType):
    """Timestamp data type.  SQL TIMESTAMP
    """
    def __init__(self, id, name):
        self.id = id
        self.name = name

    def __eq__(self, other):
        return self.id == other.id and self.name == other.name

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.id) ^ hash(self.name)


class DataTypes(object):
    """
    Utils for types
    """
    STRING = StringType()
    BOOLEAN = BooleanType()
    SHORT = ShortType()
    DOUBLE = DoubleType()
    FLOAT = FloatType()
    BYTE = ByteType()
    INT = IntegerType()
    LONG = LongType()
    CHAR = CharType()
    BYTE_ARRARY = BinaryType()
    DATE = DateType()
    TIME = TimeType()
    TIMESTAMP = TimestampType(0, "TimestampType")
    INTERVAL_MILLIS = TimestampType(1, "IntervalMillis")
    ROWTIME_INDICATOR = TimestampType(2, "RowTimeIndicator")
    PROCTIME_INDICATOR = TimestampType(3, "ProctimeTimeIndicator")
