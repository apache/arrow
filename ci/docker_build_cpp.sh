#!/usr/bin/env bash
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

source_dir=${1:-/arrow/cpp}
build_dir=${2:-/build/cpp}
install_dir=${3:-${ARROW_HOME:-/usr/local}}

export CCACHE_DIR=/build/ccache

rm -rf ${build_dir}
mkdir -p ${build_dir}
pushd ${build_dir}

cmake -GNinja \
      -DARROW_DEPENDENCY_SOURCE=${ARROW_DEPENDENCY_SOURCE:-AUTO} \
      -DARROW_VERBOSE_THIRDPARTY_BUILD=ON \
      -DCMAKE_BUILD_TYPE=${ARROW_BUILD_TYPE:-debug} \
      -DCMAKE_INSTALL_PREFIX=${install_dir} \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DARROW_WITH_BZ2=${ARROW_WITH_BZ2:-ON} \
      -DARROW_WITH_ZSTD=${ARROW_WITH_ZSTD:-ON} \
      -DARROW_BUILD_BENCHMARKS=${ARROW_BUILD_BENCHMARKS:-ON} \
      -DARROW_BOOST_VENDORED=${ARROW_BOOST_VENDORED:-OFF} \
      -DARROW_FLIGHT=${ARROW_FLIGHT:-ON} \
      -DARROW_ORC=${ARROW_ORC:-ON} \
      -DARROW_PLASMA=${ARROW_PLASMA:-ON} \
      -DARROW_PARQUET=${ARROW_PARQUET:-ON} \
      -DARROW_HDFS=${ARROW_HDFS:-OFF} \
      -DARROW_PYTHON=${ARROW_PYTHON:-OFF} \
      -DARROW_GANDIVA=${ARROW_GANDIVA:-OFF} \
      -DARROW_GANDIVA_JAVA=${ARROW_GANDIVA_JAVA:-OFF} \
      -DARROW_BUILD_TESTS=${ARROW_BUILD_TESTS:-OFF} \
      -DARROW_BUILD_UTILITIES=${ARROW_BUILD_UTILITIES:-ON} \
      -DARROW_INSTALL_NAME_RPATH=${ARROW_INSTALL_NAME_RPATH:-ON} \
      -DARROW_EXTRA_ERROR_CONTEXT=ON \
      -DARROW_BUILD_SHARED=${ARROW_BUILD_SHARED:-ON} \
      -DARROW_BUILD_STATIC=${ARROW_BUILD_STATIC:-ON} \
      -DARROW_TEST_LINKAGE=${ARROW_TEST_LINKAGE:-shared} \
      -DPARQUET_REQUIRE_ENCRYPTION=${ARROW_WITH_OPENSSL:-ON} \
      -DCMAKE_CXX_FLAGS=$CXXFLAGS \
      -Duriparser_SOURCE=AUTO \
      ${CMAKE_ARGS} \
      ${source_dir}
ninja
ninja install

popd
