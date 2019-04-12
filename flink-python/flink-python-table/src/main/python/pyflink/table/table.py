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
    A ``Table`` is the core component of the Table API.
    Similar to how the batch and streaming APIs have DataSet and DataStream,
    the Table API is built around ``Table``.

    Use the methods of ``Table`` to transform data.

    Example:
    ::
        >>> t_config = TableConfig.Builder().as_streaming_execution().set_parallelism(1).build()
        >>> t_env = TableEnvironment.get_table_environment(t_config)
        >>> t = t_env.from_collection([(1, "Bob"), (2, "Harry")], "a, b")
        >>> t.select(...)
        ...
        >>> t.insert_into("print")
        >>> t_env.execute()

    Operations such as ``join``, ``select``, ``where`` and ``group_by``
    take arguments in an expression string. Please refer to the documentation for
    the expression syntax.
    """

    def __init__(self, j_table):
        self._j_table = j_table

    @property
    def table_name(self):
        return self._j_table.tableName()

    @table_name.setter
    def table_name(self, name):
        self._j_table.tableName(name)

    def select(self, col_list):
        """
        Performs a selection operation. Similar to a SQL SELECT statement. The field expressions
        can contain complex expressions and aggregations.

        Example:
        ::
            >>> t = tab.select("key, value.avg + ' The average' as average")

        :param col_list: Expression string.
        :return: Result table.
        """
        return Table(self._j_table.select(col_list))

    def as_(self, col_list):
        """
        Renames the fields of the expression result. Use this to disambiguate fields before
        joining to operations.
        Example:
        ::
            >>> t = tab.as_("a, b")

        :param col_list: Field list expression string.
        :return: Result table.
        """
        return Table(get_method(self._j_table, "as")(col_list))

    def filter(self, predicate):
        """
        Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
        clause.
        Example:
        ::
            >>> t = tab.filter("name = 'Fred'")

        :param predicate: Predicate expression string.
        :return: Result table.
        """
        return Table(self._j_table.filter(predicate))

    def where(self, predicate):
        """
        Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
        clause.
        Example:
        ::

            >>> t = tab.where("name = 'Fred'")

        :param predicate: Predicate expression string.
        :return: Result table.
        """
        return Table(self._j_table.where(predicate))

    def group_by(self, key_list):
        """
        Groups the elements on some grouping keys. Use this before a selection with aggregations
        to perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.

        Example:
        ::
            >>> t = tab.group_by("key").select("key, value.avg")

        :param key_list: Group keys.
        :return: The grouped table.
        """
        return GroupedTable(self._j_table.groupBy(key_list))

    def distinct(self):
        """
        Removes duplicate values and returns onl
        Example:
        ::
            >>> t = tab.select("key, value").distinct()

        :return: Result table.
        """
        return Table(self._j_table.distinct())

    def join(self, right, join_predicate=None):
        """
        Joins two ``Table``\ s. Similar to a SQL join. The fields of the two joined
        operations must not overlap, use ``as_`` to rename fields if necessary. You can use
        where and select clauses after a join to further specify the behaviour of the join.

        .. note::
            Both tables must be bound to the same ``TableEnvironment`` .

        Example:
        ::
            >>> t = left.join(right).where("a = b && c > 3").select("a, b, d")
            >>> t = left.join(right, "a = b")

        :param right: Right table.
        :param join_predicate: Optional, the join predicate expression string.
        :return: Result table.
        """
        if join_predicate is not None:
            return Table(self._j_table.join(right._j_table, join_predicate))
        else:
            return Table(self._j_table.join(right._j_table))

    def left_outer_join(self, right, join_predicate=None):
        """
        Joins two ``Table``\ s. Similar to a SQL left outer join. The fields of the two joined
        operations must not overlap, use ``as_`` to rename fields if necessary.

        .. note::
            Both tables must be bound to the same ``TableEnvironment`` and its
            ``TableConfig`` must have null check enabled (default).

        Example:
        ::
            >>> t = left.left_outer_join(right).select("a, b, d")
            >>> t = left.left_outer_join(right, "a = b").select("a, b, d")

        :param right: Right table.
        :param join_predicate: Optional, the join predicate expression string.
        :return: Result table.
        """
        if join_predicate is None:
            return Table(self._j_table.leftOuterJoin(right._j_table))
        else:
            return Table(self._j_table.leftOuterJoin(
                right._j_table, join_predicate))

    def right_outer_join(self, right, join_predicate):
        """
        Joins two ``Table``\ s. Similar to a SQL right outer join. The fields of the two joined
        operations must not overlap, use ``as_`` to rename fields if necessary.

        .. note::
            Both tables must be bound to the same ``TableEnvironment`` and its
            ``TableConfig`` must have null check enabled (default).

        Example:
        ::
            >>> t = left.right_outer_join(right, "a = b").select("a, b, d")

        :param right: Right table.
        :param join_predicate: The join predicate expression string.
        :return: Result table.
        """
        return Table(self._j_table.rightOuterJoin(
            right._j_table, join_predicate))

    def full_outer_join(self, right, join_predicate):
        """
        Joins two ``Table``\ s. Similar to a SQL full outer join. The fields of the two joined
        operations must not overlap, use ``as_`` to rename fields if necessary.

        .. note::
            Both tables must be bound to the same ``TableEnvironment`` and its
            ``TableConfig`` must have null check enabled (default).

        Example:
        ::
            >>> t = left.full_outer_join(right, "a = b").select("a, b, d")

        :param right: Right table.
        :param join_predicate: The join predicate expression string.
        :return: Result table.
        """
        return Table(self._j_table.fullOuterJoin(
            right._j_table, join_predicate))

    def minus(self, right):
        """
        Minus of two ``Table``\ s with duplicate records removed.
        Similar to a SQL EXCEPT clause. Minus returns records from the left table that do not
        exist in the right table. Duplicate records in the left table are returned
        exactly once, i.e., duplicates are removed. Both tables must have identical field types.

        .. note::
            Both tables must be bound to the same ``TableEnvironment``.

        Example:
        ::
            >>> t = left.minus(right)

        :param right: Right table.
        :return: Result table.
        """
        return Table(self._j_table.minus(right._j_table))

    def minus_all(self, right):
        """
        Minus of two ``Table``\ s. Similar to a SQL EXCEPT ALL.
        Similar to a SQL EXCEPT ALL clause. MinusAll returns the records that do not exist in
        the right table. A record that is present n times in the left table and m times
        in the right table is returned (n - m) times, i.e., as many duplicates as are present
        in the right table are removed. Both tables must have identical field types.

        .. note::
            Both tables must be bound to the same ``TableEnvironment``.

        Example:
        ::
            >>> t = left.minus_all(right)

        :param right: Right table.
        :return: Result table.
        """
        return Table(self._j_table.minusAll(right._j_table))

    def union(self, right):
        """
        Unions two ``Table``\ s with duplicate records removed.
        Similar to a SQL UNION. The fields of the two union operations must fully overlap.

        .. note::
            Both tables must be bound to the same ``TableEnvironment``.

        Example:
        ::
            >>> t = left.union(right)

        :param right: Right table.
        :return: Result table.
        """
        return Table(self._j_table.union(right._j_table))

    def union_all(self, right):
        """
        Unions two ``Table``\ s. Similar to a SQL UNION ALL. The fields of the two union
        operations must fully overlap.

        .. note::
            Both tables must be bound to the same ``TableEnvironment``.

        Example:
        ::
            >>> t = left.union_all(right)

        :param right: Right table.
        :return: Result table.
        """
        return Table(self._j_table.unionAll(right._j_table))

    def intersect(self, right):
        """
        Intersects two ``Table``\ s with duplicate records removed. Intersect returns records that
        exist in both tables. If a record is present in one or both tables more than once, it is
        returned just once, i.e., the resulting table has no duplicate records. Similar to a
        SQL INTERSECT. The fields of the two intersect operations must fully overlap.

        .. note::
            Both tables must be bound to the same ``TableEnvironment``.

        Example:
        ::
            >>> t = left.intersect(right)

        :param right: Right table.
        :return: Result table.
        """
        return Table(self._j_table.intersect(right._j_table))

    def intersect_all(self, right):
        """
        Intersects two ``Table``\ s. IntersectAll returns records that exist in both tables.
        If a record is present in both tables more than once, it is returned as many times as it
        is present in both tables, i.e., the resulting table might have duplicate records. Similar
        to an SQL INTERSECT ALL. The fields of the two intersect operations must fully overlap.

        .. note::
            Both tables must be bound to the same ``TableEnvironment``.

        Example:
        ::
            >>> t = left.intersect_all(right)

        :param right: Right table.
        :return: Result table.
        """
        return Table(self._j_table.intersectAll(right._j_table))

    def order_by(self, fields):
        """
        Sorts the given ``Table``. Similar to SQL ORDER BY.
        The resulting Table is sorted globally sorted across all parallel partitions.

        Example:
        ::
            >>> t = tab.order_by("name.desc")

        :param fields: Order fields expression string,
        :return: Result table.
        """
        return Table(self._j_table.orderBy(fields))

    def offset(self, offset):
        """
        Limits a sorted result from an offset position.
        Similar to a SQL OFFSET clause. Offset is technically part of the Order By operator and
        thus must be preceded by it.
        ``Table#offset(offset)`` can be combined with a subsequent
        ``Table#fetch(fetch)`` call to return n rows after skipping the first o rows.

        Example:
        ::
            # skips the first 3 rows and returns all following rows.
            >>> t = tab.order_by("name.desc").offset(3)
            # skips the first 10 rows and returns the next 5 rows.
            >>> t = tab.order_by("name.desc").offset(10).fetch(5)

        :param offset: Number of records to skip.
        :return: Result table.
        """
        return Table(self._j_table.offset(offset))

    def fetch(self, fetch_num):
        """
        Limits a sorted result to the first n rows.
        Similar to a SQL FETCH clause. Fetch is technically part of the Order By operator and
        thus must be preceded by it.
        ``Table#fetch(fetch)`` can be combined with a preceding
        ``Table#offset(offset)`` call to return n rows after skipping the first o rows.

        Example:

        Returns the first 3 records.
        ::
            >>> t =tab.order_by("name.desc").fetch(3)

        Skips the first 10 rows and returns the next 5 rows.
        ::
            >>> t = tab.order_by("name.desc").offset(10).fetch(5)

        :param fetch_num: The number of records to return. Fetch must be >= 0.
        :return: Result table.
        """
        return Table(self._j_table.fetch(fetch_num))

    def insert_into(self, table_name):
        """
        Writes the ``Table`` to a ``TableSink`` that was registered under the specified name.

        Example:
        ::
            >>> tab.insert_into("print")

        :param table_name: Name of the ``TableSink`` to which the ``Table`` is written.
        """
        self._j_table.insertInto(table_name)


class GroupedTable(object):

    def __init__(self, java_table):
        self._j_table = java_table

    def select(self, col_list):
        """
        Performs a selection operation on a grouped table. Similar to an SQL SELECT statement.
        The field expressions can contain complex expressions and aggregations.

        Example:
        ::
            >>> t = tab.group_by("key").select("key, value.avg + ' The average' as average")


        :param col_list: Expression string that contains group keys and aggregate function calls.
        :return: Result table.
        """
        return Table(self._j_table.select(col_list))
