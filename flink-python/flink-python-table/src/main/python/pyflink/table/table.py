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

from py4j.java_gateway import get_method

__all__ = [
    'GroupedTable',
    'Table',
]


class Table(object):

    """
    Wrapper of org.apache.flink.table.api.Table
    """

    def __init__(self, java_table_or_t_env,):
        self._java_table = java_table_or_t_env

    @property
    def table_name(self):
        return self._java_table.tableName()

    @table_name.setter
    def table_name(self, name):
        self._java_table.tableName(name)

    def select(self, col_list):
        return Table(self._java_table.select(col_list))

    def as_(self, col_list):
        return Table(get_method(self._java_table, "as")(col_list))

    def filter(self, predicate):
        return Table(self._java_table.filter(predicate))

    def where(self, condition):
        return Table(self._java_table.where(condition))

    def group_by(self, key_list):
        return GroupedTable(self._java_table.groupBy(key_list))

    def distinct(self):
        return Table(self._java_table.distinct())

    def join(self, right, join_predicate=None):
        if join_predicate is not None:
            return Table(self._java_table.join(right._java_table, join_predicate))
        else:
            return Table(self._java_table.join(right._java_table))

    def left_outer_join(self, right, join_predicate=None):
        if join_predicate is None:
            return Table(self._java_table.leftOuterJoin(right._java_table))
        else:
            return Table(self._java_table.leftOuterJoin(
                right._java_table, join_predicate))

    def right_outer_join(self, right, join_predicate):
        return Table(self._java_table.rightOuterJoin(
            right._java_table, join_predicate))

    def full_outer_join(self, right, join_predicate):
        return Table(self._java_table.fullOuterJoin(
            right._java_table, join_predicate))

    def join_lateral(self, table_function_call, join_predicate=None):
        if join_predicate is None:
            return Table(self._java_table.joinLateral(table_function_call))
        else:
            return Table(self._java_table.joinLateral(table_function_call, join_predicate))

    def left_outer_join_lateral(self, table_function_call, join_predicate=None):
        if join_predicate is None:
            return Table(self._java_table.leftOuterJoinLateral(table_function_call))
        else:
            return Table(self._java_table.leftOuterJoinLateral(table_function_call, join_predicate))

    def minus(self, right):
        return Table(self._java_table.minus(right._java_table))

    def minus_all(self, right):
        return Table(self._java_table.minusAll(right._java_table))

    def union(self, right):
        return Table(self._java_table.union(right._java_table))

    def union_all(self, right):
        return Table(self._java_table.unionAll(right._java_table))

    def intersect(self, right):
        return Table(self._java_table.intersect(right._java_table))

    def intersect_all(self, right):
        return Table(self._java_table.intersectAll(right._java_table))

    def order_by(self, fields):
        return Table(self._java_table.orderBy(fields))

    def offset(self, offset):
        return Table(self._java_table.offset(offset))

    def fetch(self, fetch_num):
        return Table(self._java_table.fetch(fetch_num))

    def write_to_sink(self, j_sink,):
        self._java_table.writeToSink(j_sink._j_table_sink)

    def insert_into(self, table_name):
        return Table(self._java_table.insertInto(table_name))

    def to_string(self):
        return Table(self._java_table.toString())


class GroupedTable(object):

    """
    Wrapper of org.apache.flink.table.api.GroupedTable
    """

    def __init__(self, java_table):
        self._java_table = java_table

    def select(self, col_list):
        return Table(self._java_table.select(col_list))
