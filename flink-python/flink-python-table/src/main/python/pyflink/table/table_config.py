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
################################################################################

__all__ = ['TableConfig']


class TableConfig(object):
    """
    A config to define the runtime behavior of the Table API.
    """

    def __init__(self):
        self._is_stream = None
        self._parallelism = None

    @property
    def is_stream(self):
        return self._is_stream

    @is_stream.setter
    def is_stream(self, is_stream):
        self._is_stream = is_stream

    @property
    def parallelism(self):
        return self._parallelism

    @parallelism.setter
    def parallelism(self, parallelism):
        self._parallelism = parallelism

    class Builder(object):

        def __init__(self):
            self._is_stream = None  # type: bool
            self._parallelism = None  # type: int
            self._time_zone_id = None  # type: str
            self._null_check = None  # type: bool
            self._max_generated_code_length = None  # type: int

        def as_streaming_execution(self):
            """
            Configures streaming execution mode.
            If this method is called, :class:`StreamTableEnvironment` will be created.

            :return: :class:`TableConfig.Builder`
            """
            self._is_stream = True
            return self

        def as_batch_execution(self):
            """
            Configures batch execution mode.
            If this method is called, :class:`BatchTableEnvironment` will be created.

            :return: :class:`TableConfig.Builder`
            """
            self._is_stream = False
            return self

        def set_parallelism(self, parallelism):
            """
            Sets the parallelism for all operations.

            :param parallelism: The parallelism.
            :return: :class:`TableConfig.Builder`
            """
            self._parallelism = parallelism
            return self

        def set_time_zone(self, time_zone_id):
            """
            Sets the timezone for date/time/timestamp conversions.

            :param time_zone_id: The time zone ID in string format, either an abbreviation such as "PST",
                                 a full name such as "America/Los_Angeles", or a custom
                                 ID such as "GMT-8:00".
            :return: :class:`TableConfig.Builder`
            """
            self._time_zone_id = time_zone_id
            return self

        def set_null_check(self, null_check):
            """
            Sets the NULL check. If enabled, all fields need to be checked for NULL first.

            :param null_check: A boolean value, "True" enables NULL check and "False" disables NULL check.
            :return: :class:`TableConfig.Builder`
            """
            self._null_check = null_check
            return self

        def set_max_generated_code_length(self, max_length):
            """
            Sets the current threshold where generated code will be split into sub-function calls.
            Java has a maximum method length of 64 KB. This setting allows for finer granularity if
            necessary. Default is 64000.

            :param max_length: The maximum method length of generated java code.
            :return: :class:`TableConfig.Builder`
            """
            self._max_generated_code_length = max_length
            return self

        def build(self):
            """
            Builds :class:`TableConfig` object.

            :return: TableConfig
            """
            config = TableConfig()
            config.parallelism = self._parallelism
            config.is_stream = self._is_stream
            return config
