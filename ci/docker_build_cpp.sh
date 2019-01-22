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
set -o xtrace

source_dir=${1:-/arrow/cpp}
build_dir=${2:-/build/cpp}
install_dir=${3:-${ARROW_HOME:-/usr/local}}

mkdir -p ${build_dir}
pushd ${build_dir}

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=${ARROW_BUILD_TYPE:-debug} \
      -DCMAKE_INSTALL_PREFIX=${install_dir} \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DARROW_ORC=${ARROW_ORC:-ON} \
      -DARROW_PLASMA=${ARROW_PLASMA:-ON} \
      -DARROW_PARQUET=${ARROW_PARQUET:-ON} \
      -DARROW_HDFS=${ARROW_HDFS:-OFF} \
      -DARROW_PYTHON=${ARROW_PYTHON:-OFF} \
      -DARROW_BUILD_TESTS=${ARROW_BUILD_TESTS:-OFF} \
      -DARROW_BUILD_UTILITIES=${ARROW_BUILD_UTILITIES:-ON} \
      -DARROW_INSTALL_NAME_RPATH=${ARROW_INSTALL_NAME_RPATH:-ON} \
      -DARROW_EXTRA_ERROR_CONTEXT=ON \
      -DCMAKE_CXX_FLAGS=$CXXFLAGS \
      ${source_dir}
ninja
ninja install

popd
