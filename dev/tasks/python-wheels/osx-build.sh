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

# overrides multibuild's default build_wheel
function build_wheel {
    pip install -U pip

    # ARROW-5670: Python 3.5 can fail with HTTPS error in CMake build
    pip install setuptools_scm requests

    # Include brew installed versions of flex and bison.
    # We need them to build Thrift. The ones that come with Xcode are too old.
    export PATH="$(brew --prefix flex)/bin:$(brew --prefix bison)/bin:$PATH"

    echo `pwd`
    echo CFLAGS=${CFLAGS}
    echo CXXFLAGS=${CXXFLAGS}
    echo LDFLAGS=${LDFLAGS}

    pushd $1

    # For bzip_ep to find the osx SDK headers
    export SDKROOT="$(xcrun --show-sdk-path)"

    # Arrow is 64-bit-only at the moment
    export CFLAGS="-fPIC -arch x86_64 ${CFLAGS//"-arch i386"/}"
    export CXXFLAGS="-fPIC -arch x86_64 ${CXXFLAGS//"-arch i386"} -std=c++11"

    # We pin NumPy to an old version here as the NumPy version one builds
    # with is the oldest supported one. Thanks to NumPy's guarantees our Arrow
    # build will also work with newer NumPy versions.
    export ARROW_HOME=`pwd`/arrow-dist
    export PARQUET_HOME=`pwd`/arrow-dist

    pip install $(pip_opts) -r python/requirements-wheel-build.txt

    git submodule update --init
    export ARROW_TEST_DATA=`pwd`/testing/data

    pushd cpp
    mkdir build
    pushd build
    cmake -DARROW_BUILD_SHARED=ON \
          -DARROW_BUILD_STATIC=OFF \
          -DARROW_BUILD_TESTS=OFF \
          -DARROW_DATASET=ON \
          -DARROW_DEPENDENCY_SOURCE=BUNDLED \
          -DARROW_HDFS=ON \
          -DARROW_FLIGHT=ON \
          -DARROW_GANDIVA=OFF \
          -DARROW_GRPC_USE_SHARED=OFF \
          -DARROW_JEMALLOC=ON \
          -DARROW_ORC=OFF \
          -DARROW_PARQUET=ON \
          -DARROW_PLASMA=ON \
          -DARROW_PROTOBUF_USE_SHARED=OFF \
          -DARROW_PYTHON=ON \
          -DARROW_RPATH_ORIGIN=ON \
          -DARROW_VERBOSE_THIRDPARTY_BUILD=ON \
          -DARROW_WITH_BROTLI=ON \
          -DARROW_WITH_BZ2=ON \
          -DARROW_WITH_LZ4=ON \
          -DARROW_WITH_SNAPPY=ON \
          -DARROW_WITH_ZLIB=ON \
          -DARROW_WITH_ZSTD=ON \
          -DBOOST_SOURCE=SYSTEM \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
          -DgRPC_SOURCE=SYSTEM \
          -DLLVM_SOURCE=SYSTEM \
          -DMAKE=make \
          -DOPENSSL_USE_STATIC_LIBS=ON \
          -DProtobuf_SOURCE=SYSTEM \
          ..
    make -j$(sysctl -n hw.logicalcpu)
    make install
    popd
    popd

    # Unset the HOME variables and use pkg-config to discover the previously
    # built binaries. By using pkg-config, we also are able to discover the
    # ABI and SO versions of the dynamic libraries.
    export PKG_CONFIG_PATH=${ARROW_HOME}/lib/pkgconfig:${PARQUET_HOME}/lib/pkgconfig:${PKG_CONFIG_PATH}
    unset ARROW_HOME
    unset PARQUET_HOME

    export PYARROW_WITH_DATASET=1
    export PYARROW_WITH_FLIGHT=1
    export PYARROW_WITH_HDFS=1
    export PYARROW_WITH_PLASMA=1
    export PYARROW_WITH_PARQUET=1
    export PYARROW_WITH_ORC=0
    export PYARROW_WITH_JEMALLOC=1
    export PYARROW_WITH_PLASMA=1
    export PYARROW_WITH_GANDIVA=0
    export PYARROW_BUNDLE_ARROW_CPP=1
    export PYARROW_BUILD_TYPE='release'
    export PYARROW_INSTALL_TESTS=1
    export SETUPTOOLS_SCM_PRETEND_VERSION=$PYARROW_VERSION
    pushd python
    python setup.py build_ext bdist_wheel
    ls -l dist/
    popd

    popd
}

function install_wheel {
    multibuild_dir=`realpath $MULTIBUILD_DIR`

    pushd $1  # enter arrow's directory
    wheelhouse="$PWD/python/dist"

    # Install wheel
    pip install $(pip_opts) $wheelhouse/*.whl

    popd
}

function run_unit_tests {
    pushd $1

    export PYARROW_TEST_CYTHON=OFF

    # Install test dependencies
    pip install $(pip_opts) -r python/requirements-wheel-test.txt

    # Run pyarrow tests
    pytest -rs --pyargs pyarrow

    popd
}

function run_import_tests {
    # Test optional dependencies
    python -c "
import pyarrow
import pyarrow.parquet
import pyarrow.plasma
import pyarrow.fs
import pyarrow._hdfs
import pyarrow.dataset
import pyarrow.flight
"
}
