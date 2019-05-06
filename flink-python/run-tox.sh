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

function install_miniconda() {
    wget $1  -O  $2
}

function environment_install() {

    sysOS=`uname -s`
    support_os=("Darwin" "Linux")
    os_to_conda_url=("https://repo.continuum.io/miniconda/Miniconda3-latest-MacOSX-x86_64.sh" "https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh" )
    SUPPORT_OS="False"
    if echo "${support_os[@]}" | grep -w $sysOS &> /dev/null; then
        SUPPORT_OS="True"
    fi

    if [ $SUPPORT_OS == "False" ]; then
        echo "Can't support the environment" $sysOS
        exit 1;
    fi

    local os_index
    for ((i=0;i<${#support_os[@]};i++)) do
        if [ ${support_os[i]} == $sysOS ]; then
            os_index=i
            break
        fi
    done

    echo "installing miniconda..."
    create_dir $CURRENT_DIR/download
    conda_install_sh="$CURRENT_DIR/download/miniconda.sh"
    if [ ! -f "$conda_install_sh" ]; then
        install_miniconda ${os_to_conda_url[os_index]} $conda_install_sh
        chmod +x $conda_install_sh
        if [ -d "$CURRENT_DIR/.conda" ]; then
            rm -rf "$CURRENT_DIR/.conda"
        fi
    fi
    if [ ! -d "$CURRENT_DIR/.conda" ]; then
        $conda_install_sh -b -p $CURRENT_DIR/.conda &> /dev/null
    fi
    echo "finishing install miniconda"

    echo "installing python environment..."
    py_env_name=("py34" "py35" "py36" "py37" "py27")
    py_env=("3.4" "3.5" "3.6" "3.7" "2.7")
    current_py_env=$( $CONDA_BUILD env list )
    for ((i=0;i<${#py_env[@]};i++)) do
        if [ ! -d "$CURRENT_DIR/.conda/envs/${py_env_name[i]}" ]; then
            expect -c "
            set timeout -1;
            spawn ${CONDA_BUILD} create --name ${py_env_name[i]} python=${py_env[i]};
            expect /n)?;
            send y\r;
            interact;"
        fi
    done
    echo "finishing install python environment"
}

function install_tox() {
    echo "installing tox..."
    if [ ! -f "$CURRENT_DIR/.conda/bin/tox" ]; then
        expect -c "
        set timeout -1;
        spawn ${CONDA_BUILD} install -c conda-forge tox;
        expect /n)?;
        send y\r;
        interact;"
    fi
    echo "finishing install tox"
}

function activate () {
    for py_dir in ${CURRENT_DIR}/.conda/envs/*
    do
        PATH=$py_dir/bin:$PATH
    done
    export PATH
}

function clean_tox() {
    find . \( -path ./dev -o -path ./.tox \) -prune -o -type d -name "__pycache__" -print | xargs rm -rf
    find . \( -path ./dev -o -path ./.tox \) -prune -o -type d -name "test_tox.egg-info" -print | xargs rm -rf
}

function create_dir() {
    if [ ! -d $1 ]; then
        mkdir $1
    fi
}

function tox_check() {
    echo "tox check start"
    TOX_BUILD=$CURRENT_DIR/.conda/bin/tox
    LOG_DIR=$CURRENT_DIR/log
    create_dir "$LOG_DIR"
    PYTHONDONTWRITEBYTECODE=1 $TOX_BUILD -c $CURRENT_DIR/tox.ini --recreate > $LOG_DIR/tox_check_result.log
    if [ `grep -c "congratulations :)" $LOG_DIR/tox_check_result.log` -eq '0' ]; then
        echo "tox checked failed, the detailed information is in $LOG_DIR/tox_check_result.log"
    else
        echo "tox checked success"
    fi
    clean_tox
}

function deactivate() {
    # reset old environment variables
    # ! [ -z ${VAR+_} ] returns true if VAR is declared at all
    if ! [ -z "${_OLD_PATH+_}" ] ; then
        PATH="$_OLD_PATH"
        export PATH
        unset _OLD_PATH
    fi
}

function pycodestyle_test {
    local PYCODESTYLE_STATUS=
    local PYCODESTYLE_REPORT=
    local RUN_LOCAL_PYCODESTYLE=
    local VERSION=
    local EXPECTED_PYCODESTYLE=
    local PYCODESTYLE_SCRIPT_PATH="$CURRENT_DIR/download/pycodestyle-$MINIMUM_PYCODESTYLE.py"
    local PYCODESTYLE_SCRIPT_REMOTE_PATH="https://raw.githubusercontent.com/PyCQA/pycodestyle/$MINIMUM_PYCODESTYLE/pycodestyle.py"

    if [[ ! "$1" ]]; then
        echo "No python files found!  Something is very wrong -- exiting."
        exit 1;
    fi

    # check for locally installed pycodestyle & version
    RUN_LOCAL_PYCODESTYLE="False"
    if hash "$PYCODESTYLE_BUILD" 2> /dev/null; then
        VERSION=$( $PYCODESTYLE_BUILD --version 2> /dev/null)
        EXPECTED_PYCODESTYLE=$( (python -c 'from distutils.version import LooseVersion;
                                print(LooseVersion("""'${VERSION[0]}'""") >= LooseVersion("""'$MINIMUM_PYCODESTYLE'"""))')\
                                2> /dev/null)

        if [ "$EXPECTED_PYCODESTYLE" == "True" ]; then
            RUN_LOCAL_PYCODESTYLE="True"
        fi
    fi

    # download the right version or run locally
    if [ $RUN_LOCAL_PYCODESTYLE == "False" ]; then
        # Get pycodestyle at runtime so that we don't rely on it being installed on the build server.
        # See: https://github.com/apache/spark/pull/1744#issuecomment-50982162
        # Updated to the latest official version of pep8. pep8 is formally renamed to pycodestyle.
        # echo "downloading pycodestyle from $PYCODESTYLE_SCRIPT_REMOTE_PATH..."
        if [ ! -e "$PYCODESTYLE_SCRIPT_PATH" ]; then
            curl --silent -o "$PYCODESTYLE_SCRIPT_PATH" "$PYCODESTYLE_SCRIPT_REMOTE_PATH"
            local curl_status="$?"

            if [ "$curl_status" -ne 0 ]; then
                echo "Failed to download pycodestyle.py from $PYCODESTYLE_SCRIPT_REMOTE_PATH"
                exit "$curl_status"
            fi
        fi

        echo "starting pycodestyle test..."
        PYCODESTYLE_REPORT=$( (python "$PYCODESTYLE_SCRIPT_PATH" --config=tox.ini $1) 2>&1)
        PYCODESTYLE_STATUS=$?
    else
        # we have the right version installed, so run locally
        echo "starting pycodestyle test..."
        PYCODESTYLE_REPORT=$( ($PYCODESTYLE_BUILD --config=tox.ini $1) 2>&1)
        PYCODESTYLE_STATUS=$?
    fi

    LOG_DIR=$CURRENT_DIR/log
    create_dir $LOG_DIR
    if [ $PYCODESTYLE_STATUS -ne 0 ]; then
        echo "pycodestyle checks failed: detailed infos in $LOG_DIR/pycodestyle_result.log"
        echo "$PYCODESTYLE_REPORT">$LOG_DIR/pycodestyle_result.log
    else
        echo "pycodestyle checks passed."
        echo "pycodestyle checks passed.">$LOG_DIR/pycodestyle_result.log
    fi
}

CURRENT_DIR="$( cd "$( dirname "$0" )" && pwd )"
FLINK_ROOT_DIR=$(dirname "$CURRENT_DIR")
CONDA_BUILD=$CURRENT_DIR/.conda/bin/conda
environment_install
_OLD_PATH="$PATH"
VIRTUAL_ENV=$CURRENT_DIR/.conda/envs
pushd "$FLINK_ROOT_DIR" &> /dev/null
activate
install_tox
tox_check
PYCODESTYLE_BUILD="pycodestyle"
MINIMUM_PYCODESTYLE="2.4.0"
PYTHON_SOURCE="$(find . \( -path ./dev -o -path ./.tox \) -prune -o -type f -name "*.py" -print )"
pycodestyle_test "$PYTHON_SOURCE"
deactivate
