#!/bin/bash -e
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

# Build upon the scripts in https://github.com/matthew-brett/manylinux-builds
# * Copyright (c) 2013-2016, Matt Terry and Matthew Brett (BSD 2-clause)

PYTHON_VERSIONS="${PYTHON_VERSIONS:-2.7,16 2.7,32 3.4,16 3.5,16 3.6,16}"

source /multibuild/manylinux_utils.sh

for PYTHON_TUPLE in ${PYTHON_VERSIONS}; do
    IFS=","
    set -- $PYTHON_TUPLE;
    PYTHON=$1
    U_WIDTH=$2
    PYTHON_INTERPRETER="$(cpython_path $PYTHON ${U_WIDTH})/bin/python"
    PIP="$(cpython_path $PYTHON ${U_WIDTH})/bin/pip"
    PATH="$PATH:$(cpython_path $PYTHON ${U_WIDTH})"

    echo "=== (${PYTHON}, ${U_WIDTH}) Installing build dependencies ==="
    $PIP install "numpy==1.10.4"
    $PIP install "cython==0.28.1"
    $PIP install "pandas==0.20.3"
    $PIP install "virtualenv==15.1.0"

    echo "=== (${PYTHON}, ${U_WIDTH}) Preparing virtualenv for tests ==="
    "$(cpython_path $PYTHON ${U_WIDTH})/bin/virtualenv" -p ${PYTHON_INTERPRETER} --no-download /venv-test-${PYTHON}-${U_WIDTH}
    source /venv-test-${PYTHON}-${U_WIDTH}/bin/activate
    pip install pytest 'numpy==1.14.0' 'pandas==0.20.3'
    deactivate
done

# Remove debug symbols from libraries that were installed via wheel.
find /venv-test-*/lib/*/site-packages/pandas -name '*.so' -exec strip '{}' ';'
find /venv-test-*/lib/*/site-packages/numpy -name '*.so' -exec strip '{}' ';'
find /opt/_internal/cpython-*/lib/*/site-packages/pandas -name '*.so' -exec strip '{}' ';'
# Only Python 3.6 packages are stripable as they are built inside of the image 
find /opt/_internal/cpython-3.6.4/lib/python3.6/site-packages/numpy -name '*.so' -exec strip '{}' ';'
find /opt/_internal/*/lib/*/site-packages/Cython -name '*.so' -exec strip '{}' ';'

# Remove pip cache again. It's useful during the virtualenv creation but we
# don't want it persisted in the docker layer, ~264MiB
rm -rf /root/.cache
# Remove pandas' tests module as it includes a lot of data, ~27MiB per Python
# venv, i.e. 216MiB in total
rm -rf /opt/_internal/*/lib/*/site-packages/pandas/tests
rm -rf /venv-test-*/lib/*/site-packages/pandas/tests
