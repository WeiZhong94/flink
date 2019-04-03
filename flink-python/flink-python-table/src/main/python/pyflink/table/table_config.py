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

__all__ = ['TableConfig']


class TableConfig(object):

    def __init__(self, j_conf=None):
        self._is_stream = True
        self._parallelism = None
        self._j_table_config = j_conf

    @property
    def is_stream(self):
        return self._is_stream

    @is_stream.setter
    def is_stream(self, value):
        self._is_stream = value

    @property
    def parallelism(self):
        return self._parallelism

    @parallelism.setter
    def parallelism(self, value):
        self._parallelism = value

    class Builder(object):

        def __init__(self):
            self.is_stream = None
            self.parallelism = None

        def as_streaming_execution(self):
            self.is_stream = True
            return self

        def as_batch_execution(self):
            self.is_stream = False
            return self

        def set_parallelism(self, parallelism):
            self.parallelism = parallelism
            return self

        def build(self):
            table_conf = TableConfig()
            table_conf.is_stream = self.is_stream
            table_conf.parallelism = self.parallelism
            return table_conf
