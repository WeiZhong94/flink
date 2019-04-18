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
import os
import shutil
import signal
import struct
import tempfile
import time
from subprocess import Popen, PIPE
from threading import RLock

from py4j.java_gateway import JavaGateway, GatewayClient

_gateway = None
_lock = RLock()
_execute_mode_param = '--mini-cluster'

def get_gateway():
    global _gateway
    global _lock
    with _lock:
        # if Java Gateway is ready(in this case, python is started by java)
        if _gateway is None:
            if 'PYFLINK_GATEWAY_PORT' in os.environ:
                gateway_port = int(os.environ['PYFLINK_GATEWAY_PORT'])
                _gateway = JavaGateway(GatewayClient(port=gateway_port), auto_convert=True)
            else:
                # we start Java from python
                _gateway = launch_java_gateway()
    return _gateway


def receive_all(sock, data_len):
    chunks = []
    bytes_recd = 0
    while bytes_recd < data_len:
        chunk = sock.recv(min(data_len - bytes_recd, 2048))
        if chunk == b'':
            raise RuntimeError("socket connection broken")
        chunks.append(chunk)
        bytes_recd = bytes_recd + len(chunk)
    return b''.join(chunks)


def launch_java_gateway():

    java_port = None
    # if not started yet, start it.
    if java_port is None:
        conn_info_dir = tempfile.mkdtemp()
        try:
            fd, conn_info_file = tempfile.mkstemp(dir=conn_info_dir)
            os.close(fd)
            os.unlink(conn_info_file)

            launch_java_process(conn_info_file)

            with open(conn_info_file, "rb") as info:
                java_port = struct.unpack("!I", info.read(4))[0]
        finally:
            shutil.rmtree(conn_info_dir)

    # Connect to the java gateway
    gateway = JavaGateway(GatewayClient(port=java_port), auto_convert=True)

    return gateway


def launch_java_process(conn_info_file):
    flink_home = None
    if 'FLINK_ROOT_DIR' in os.environ:
        flink_home = os.environ['FLINK_ROOT_DIR']
    elif 'FLINK_HOME' in os.environ:
        flink_home = os.environ['FLINK_HOME']
    if flink_home is None:
        raise Exception('FLINK_ROOT_DIR or FLINK_HOME is not set')
    bin_dir = flink_home + '/bin'

    shell_gateway = ClassName.PYTHON_SHELL_GATEWAY_SERVER
    command = [bin_dir+'/pyflink2.sh', _execute_mode_param, '-c', shell_gateway, conn_info_file]

    def preexec_func():
        # ignore SIGINT
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    p = Popen(command, stdin=PIPE, preexec_fn=preexec_func, env=dict(os.environ))

    while not p.poll() and not os.path.isfile(conn_info_file):
        time.sleep(0.1)

    if not os.path.isfile(conn_info_file):
        raise Exception("Java gateway process exited before sending its port number")
    return p


class ClassName(object):
    STRING = "java.lang.String"
    DATE = "java.sql.Date"
    TIME = "java.sql.Time"
    TIMESTAMP = "java.sql.Timestamp"
    TUPLE = "org.apache.flink.api.java.tuple.Tuple"
    TYPES = "org.apache.flink.api.common.typeinfo.Types"
    TYPE_INFORMATION = "org.apache.flink.api.common.typeinfo.TypeInformation"
    TIME_INDICATOR_TYPE_INFO = "org.apache.flink.table.typeutils.TimeIndicatorTypeInfo"
    ROW_TYPE_INFO = "org.apache.flink.api.java.typeutils.RowTypeInfo"
    CSV_TABLE_SOURCE = "org.apache.flink.table.sources.CsvTableSource"
    CSV_TABLE_SINK = "org.apache.flink.table.sinks.CsvTableSink"
    WRITE_MODE = "org.apache.flink.core.fs.FileSystem.WriteMode"
    TABLE_ENVIRONMENT = "org.apache.flink.table.api.TableEnvironment"
    EXECUTION_ENVIRONMENT = "org.apache.flink.api.java.ExecutionEnvironment"
    STREAM_EXECUTION_ENVIRONMENT = "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment"
    PYTHON_SHELL_GATEWAY_SERVER = "org.apache.flink.api.python.PythonShellGatewayServer"

