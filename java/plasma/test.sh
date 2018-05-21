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
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
  PARALLEL=$(nproc)
elif [[ "$unamestr" == "Darwin" ]]; then
  PARALLEL=$(sysctl -n hw.ncpu)
else
  echo "Unrecognized platform."
  exit 1
fi
pushd ../../cpp
    if [ ! -d "release" ]; then
      mkdir release
    fi
    pushd release
        cmake -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_C_FLAGS="-g -O3" \
            -DCMAKE_CXX_FLAGS="-g -O3" \
            -DARROW_BUILD_TESTS=off \
            -DARROW_HDFS=on \
            -DARROW_BOOST_USE_SHARED=off \
            -DARROW_PYTHON=on \
            -DARROW_PLASMA=on \
            -DPLASMA_PYTHON=on \
            -DARROW_JEMALLOC=off \
            -DARROW_WITH_BROTLI=off \
            -DARROW_WITH_LZ4=off \
            -DARROW_WITH_ZLIB=off \
            -DARROW_WITH_ZSTD=off \
            -DARROW_PLASMA_JAVA_CLIENT=on \
            ..
        make VERBOSE=1 -j$PARALLEL
    popd
popd

mvn clean install
export PLASMA_STORE=$ROOT_DIR/../../cpp/release/release/plasma_store
java -cp target/test-classes:target/classes -Djava.library.path=$ROOT_DIR/../../cpp/release/release/ org.apache.arrow.plasma.PlasmaClientTest
