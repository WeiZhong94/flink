#!/usr/bin/env bash
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

# =====================================================================
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

FLINK_CLASSPATH=`constructFlinkClassPath`

ARGS=()
EXECUTE_MODE="local"

# if shell mode has been specified, use shell-driver
while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
        -c|--class)
            DRIVER=$2
            shift
            shift
            ;;
        --mini-cluster)
            EXECUTE_MODE="mini"
            shift
            ;;
        *)
           ARGS+=("$1")
           shift
           ;;
    esac
done

export PYTHON_JAR_PATH=`echo "$FLINK_ROOT_DIR"/opt/flink-python-table*.jar`
if [[ "$EXECUTE_MODE" == "local" ]]; then
    exec "$FLINK_BIN_DIR"/flink run -c ${DRIVER} ${PYTHON_JAR_PATH} ${ARGS[@]}
elif [[ "$EXECUTE_MODE" == "mini" ]]; then
    exec $JAVA_RUN $JVM_ARGS -cp ${FLINK_CLASSPATH}:${PYTHON_JAR_PATH} ${DRIVER} ${ARGS[@]}
fi


