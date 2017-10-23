#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e
set -x

ROOT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

# Current cpp-coveralls version 0.4 throws an error (PARQUET-1075) on Travis
# CI. Pin to last working version
pip install cpp_coveralls==0.3.12

mkdir coverage_artifacts
python $TRAVIS_BUILD_DIR/cpp/build-support/collect_coverage.py \
       $ARROW_CPP_BUILD_DIR \
       coverage_artifacts

cd coverage_artifacts

ls -l

coveralls --gcov $(which gcov-4.9) \
    --gcov-options '\-l' --root '' \
    --include $ARROW_CPP_BUILD_DIR \
    --exclude $ARROW_CPP_BUILD_DIR/boost_ep \
    --exclude $ARROW_CPP_BUILD_DIR/boost_ep-prefix \
    --exclude $ARROW_CPP_BUILD_DIR/brotli_ep \
    --exclude $ARROW_CPP_BUILD_DIR/brotli_ep-prefix \
    --exclude $ARROW_CPP_BUILD_DIR/flatbuffers_ep \
    --exclude $ARROW_CPP_BUILD_DIR/flatbuffers_ep-prefix \
    --exclude $ARROW_CPP_BUILD_DIR/gbenchmark_ep \
    --exclude $ARROW_CPP_BUILD_DIR/gbenchmark_ep-prefix \
    --exclude $ARROW_CPP_BUILD_DIR/gflags_ep \
    --exclude $ARROW_CPP_BUILD_DIR/gflags_ep-prefix \
    --exclude $ARROW_CPP_BUILD_DIR/googletest_ep \
    --exclude $ARROW_CPP_BUILD_DIR/googletest_ep-prefix \
    --exclude $ARROW_CPP_BUILD_DIR/grpc_ep \
    --exclude $ARROW_CPP_BUILD_DIR/grpc_ep-prefix \
    --exclude $ARROW_CPP_BUILD_DIR/jemalloc_ep \
    --exclude $ARROW_CPP_BUILD_DIR/jemalloc_ep-prefix \
    --exclude $ARROW_CPP_BUILD_DIR/lz4_ep \
    --exclude $ARROW_CPP_BUILD_DIR/lz4_ep-prefix \
    --exclude $ARROW_CPP_BUILD_DIR/rapidjson_ep \
    --exclude $ARROW_CPP_BUILD_DIR/rapidjson_ep-prefix \
    --exclude $ARROW_CPP_BUILD_DIR/snappy_ep \
    --exclude $ARROW_CPP_BUILD_DIR/snappy_ep-prefix \
    --exclude $ARROW_CPP_BUILD_DIR/zlib_ep \
    --exclude $ARROW_CPP_BUILD_DIR/zlib_ep-prefix \
    --exclude $ARROW_CPP_BUILD_DIR/zstd_ep \
    --exclude $ARROW_CPP_BUILD_DIR/zstd_ep-prefix \
    --exclude /usr
