#!/bin/bash
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

set -e

#----------------------------------------------------------------------
# Change this to whatever makes sense for your system

WORKDIR=${WORKDIR:-$HOME}
MINICONDA=$WORKDIR/miniconda-for-arrow
LIBRARY_INSTALL_DIR=$WORKDIR/local-libs
CPP_BUILD_DIR=$WORKDIR/arrow-cpp-build
ARROW_ROOT=$WORKDIR/arrow
export ARROW_HOME=$WORKDIR/dist
export LD_LIBRARY_PATH=$ARROW_HOME/lib:$LD_LIBRARY_PATH

virtualenv $WORKDIR/venv
source $WORKDIR/venv/bin/activate

git clone https://github.com/apache/arrow.git $ARROW_ROOT

pip install -r $ARROW_ROOT/python/requirements-build.txt \
     -r $ARROW_ROOT/python/requirements-test.txt

#----------------------------------------------------------------------
# Build C++ library

mkdir -p $CPP_BUILD_DIR
pushd $CPP_BUILD_DIR

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=DEBUG \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DARROW_WITH_BZ2=ON \
      -DARROW_WITH_ZLIB=ON \
      -DARROW_WITH_ZSTD=ON \
      -DARROW_WITH_LZ4=ON \
      -DARROW_WITH_SNAPPY=ON \
      -DARROW_WITH_BROTLI=ON \
      -DARROW_PARQUET=ON \
      -DARROW_PYTHON=ON \
      $ARROW_ROOT/cpp

ninja install

popd

#----------------------------------------------------------------------
# Build and test Python library
pushd $ARROW_ROOT/python

rm -rf build/  # remove any pesky pre-existing build directory

export PYARROW_BUILD_TYPE=Debug
export PYARROW_CMAKE_GENERATOR=Ninja
export PYARROW_WITH_PARQUET=1

# You can run either "develop" or "build_ext --inplace". Your pick

# python setup.py build_ext --inplace
python setup.py develop

# git submodules are required for unit tests
git submodule update --init
export PARQUET_TEST_DATA="$ARROW_ROOT/cpp/submodules/parquet-testing/data"
export ARROW_TEST_DATA="$ARROW_ROOT/testing/data"

py.test pyarrow
