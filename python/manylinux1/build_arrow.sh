#!/bin/bash
#
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
#
# Usage:
#   docker run --rm -v $PWD:/io arrow-base-x86_64 /io/build_arrow.sh
# or with Parquet support
#   docker run --rm -v $PWD:/io parquet_arrow-base-x86_64 /io/build_arrow.sh

# Build upon the scripts in https://github.com/matthew-brett/manylinux-builds
# * Copyright (c) 2013-2016, Matt Terry and Matthew Brett (BSD 2-clause)

PYTHON_VERSIONS="${PYTHON_VERSIONS:-2.7 3.4 3.5 3.6}"

# Package index with only manylinux1 builds
MANYLINUX_URL=https://nipy.bic.berkeley.edu/manylinux

source /multibuild/manylinux_utils.sh

# Quit on failure
set -e

cd /arrow/python

# PyArrow build configuration
export PYARROW_BUILD_TYPE='release'
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_STATIC_PARQUET=1
export PYARROW_WITH_PLASMA=1
export PYARROW_BUNDLE_ARROW_CPP=1
export PKG_CONFIG_PATH=/arrow-dist/lib64/pkgconfig
export PYARROW_CMAKE_OPTIONS='-DTHRIFT_HOME=/usr'
# Ensure the target directory exists
mkdir -p /io/dist

for PYTHON in ${PYTHON_VERSIONS}; do
    PYTHON_INTERPRETER="$(cpython_path $PYTHON)/bin/python"
    PIP="$(cpython_path $PYTHON)/bin/pip"
    PIPI_IO="$PIP install -f $MANYLINUX_URL"
    PATH="$PATH:$(cpython_path $PYTHON)"

    echo "=== (${PYTHON}) Building Arrow C++ libraries ==="
    ARROW_BUILD_DIR=/arrow/cpp/build-PY${PYTHON}
    mkdir -p "${ARROW_BUILD_DIR}"
    pushd "${ARROW_BUILD_DIR}"
    PATH="$(cpython_path $PYTHON)/bin:$PATH" cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/arrow-dist -DARROW_BUILD_TESTS=OFF -DARROW_BUILD_SHARED=ON -DARROW_BOOST_USE_SHARED=OFF -DARROW_JEMALLOC=off -DARROW_RPATH_ORIGIN=ON -DARROW_JEMALLOC_USE_SHARED=OFF -DARROW_PYTHON=ON -DPythonInterp_FIND_VERSION=${PYTHON} -DARROW_PLASMA=ON ..
    make -j5 install
    popd

    # Clear output directory
    rm -rf dist/
    echo "=== (${PYTHON}) Building wheel ==="
    PATH="$PATH:$(cpython_path $PYTHON)/bin" $PYTHON_INTERPRETER setup.py build_ext --inplace --with-parquet --with-static-parquet --bundle-arrow-cpp
    PATH="$PATH:$(cpython_path $PYTHON)/bin" $PYTHON_INTERPRETER setup.py bdist_wheel

    echo "=== (${PYTHON}) Test the existence of optional modules ==="
    $PIPI_IO -r requirements.txt
    PATH="$PATH:$(cpython_path $PYTHON)/bin" $PYTHON_INTERPRETER -c "import pyarrow.parquet"
    PATH="$PATH:$(cpython_path $PYTHON)/bin" $PYTHON_INTERPRETER -c "import pyarrow.plasma"

    echo "=== (${PYTHON}) Tag the wheel with manylinux1 ==="
    mkdir -p repaired_wheels/
    auditwheel -v repair -L . dist/pyarrow-*.whl -w repaired_wheels/

    echo "=== (${PYTHON}) Testing manylinux1 wheel ==="
    source /venv-test-${PYTHON}/bin/activate
    pip install repaired_wheels/*.whl

    py.test --parquet /venv-test-${PYTHON}/lib/*/site-packages/pyarrow -v
    deactivate

    mv repaired_wheels/*.whl /io/dist
done
