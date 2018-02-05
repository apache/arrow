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

PYTHON_VERSIONS="${PYTHON_VERSIONS:-2.7 3.4 3.5 3.6}"

# Package index with only manylinux1 builds
MANYLINUX_URL=https://nipy.bic.berkeley.edu/manylinux

source /multibuild/manylinux_utils.sh

for PYTHON in ${PYTHON_VERSIONS}; do
    PYTHON_INTERPRETER="$(cpython_path $PYTHON)/bin/python"
    PIP="$(cpython_path $PYTHON)/bin/pip"
    PIPI_IO="$PIP install -f $MANYLINUX_URL"
    PATH="$PATH:$(cpython_path $PYTHON)"

    echo "=== (${PYTHON}) Installing build dependencies ==="
    $PIPI_IO "numpy==1.10.1"
    $PIPI_IO "cython==0.25.2"
    $PIPI_IO "pandas==0.20.1"
    $PIPI_IO "virtualenv==15.1.0"

    echo "=== (${PYTHON}) Preparing virtualenv for tests ==="
    "$(cpython_path $PYTHON)/bin/virtualenv" -p ${PYTHON_INTERPRETER} --no-download /venv-test-${PYTHON}
    source /venv-test-${PYTHON}/bin/activate
    pip install pytest 'numpy==1.12.1' 'pandas==0.20.1'
    deactivate
done

# Remove pip cache again. It's useful during the virtualenv creation but we
# don't want it persisted in the docker layer, ~264MiB
rm -rf /root/.cache
# Remove pandas' tests module as it includes a lot of data, ~27MiB per Python
# venv, i.e. 216MiB in total
rm -rf /opt/_internal/*/lib/*/site-packages/pandas/tests
rm -rf /venv-test-*/lib/*/site-packages/pandas/tests
