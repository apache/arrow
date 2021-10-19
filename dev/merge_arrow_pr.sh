#!/usr/bin/env sh

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Wrapper script that automatically creates a Python virtual environment
# and runs merge_arrow_pr.py inside it.

set -e

PYTHON=$(which python3)
PYVER=$($PYTHON -c "import sys; print('.'.join(map(str, sys.version_info[:2])))")

GIT_ROOT=$(git rev-parse --show-toplevel)
ENV_DIR=$GIT_ROOT/dev/.venv$PYVER

ENV_PYTHON=$ENV_DIR/bin/python3
ENV_PIP="$ENV_PYTHON -m pip --no-input"

check_venv() {
    [ -x $ENV_PYTHON ] || {
        echo "Virtual environment broken: $ENV_PYTHON not an executable"
        exit 1
    }
}

create_venv() {
    echo ""
    echo "Creating Python virtual environment in $ENV_DIR ..."
    echo ""
    $PYTHON -m venv $ENV_DIR
    $ENV_PIP install -q -r $GIT_ROOT/dev/requirements_merge_arrow_pr.txt || {
        echo "Failed to setup virtual environment"
        echo "Please delete directory '$ENV_DIR' and try again"
        exit $?
    }
}

[ -d $ENV_DIR ] || create_venv
check_venv

$ENV_PYTHON $GIT_ROOT/dev/merge_arrow_pr.py "$@"
