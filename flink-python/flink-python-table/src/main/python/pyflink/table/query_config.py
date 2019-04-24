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
from abc import ABCMeta
from datetime import timedelta

from pyflink.java_gateway import get_gateway


class QueryConfig(object):

    __metaclass__ = ABCMeta

    def __init__(self, j_query_config):
        self._j_query_config = j_query_config


class StreamQueryConfig(QueryConfig):

    def __init__(self, j_stream_query_config=None):
        self._jvm = get_gateway().jvm
        if j_stream_query_config is not None:
            self._j_stream_query_config = j_stream_query_config
        else:
            self._j_stream_query_config = self._jvm.StreamQueryConfig()
        super(StreamQueryConfig, self).__init__(self._j_stream_query_config)

    def with_idle_state_retention_time(self, min_time, max_time):
        #  type: (timedelta, timedelta) -> StreamQueryConfig
        j_time_class = self._jvm.org.apache.flink.api.common.time.Time
        j_min_time = j_time_class.milliseconds(round(min_time.total_seconds() * 1000))
        j_max_time = j_time_class.milliseconds(round(max_time.total_seconds() * 1000))
        self._j_stream_query_config = \
            self._j_stream_query_config.withIdleStateRetentionTime(j_min_time, j_max_time)
        return self

    def get_min_idle_state_retention_time(self):
        #  type: () -> int
        return self._j_stream_query_config.getMinIdleStateRetentionTime()

    def get_max_idle_state_retention_time(self):
        #  type: () -> int
        return self._j_stream_query_config.getMaxIdleStateRetentionTime()


class BatchQueryConfig(QueryConfig):

    def __init__(self, j_batch_query_config=None):
        self._jvm = get_gateway().jvm
        if j_batch_query_config is not None:
            self._j_batch_query_config = j_batch_query_config
        else:
            self._j_batch_query_config = self._jvm.BatchQueryConfig()
        super(BatchQueryConfig, self).__init__(self._j_batch_query_config)
