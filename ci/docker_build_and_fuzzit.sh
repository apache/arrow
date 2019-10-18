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

set -euxo pipefail

export ARROW_FUZZING="ON"
#export ARROW_DEPENDENCY_SOURCE="BUNDLED"
export ARROW_USE_ASAN="ON"
export CC="clang-7"
export CXX="clang++-7"
export ARROW_BUILD_TYPE="RelWithDebInfo"
export ARROW_FLIGHT="OFF"
export ARROW_GANDIVA="OFF"
export ARROW_ORC="OFF"
export ARROW_PARQUET="OFF"
export ARROW_PLASMA="OFF"
export ARROW_WITH_BZ2="OFF"
export ARROW_WITH_ZSTD="OFF"
export ARROW_BUILD_BENCHMARKS="OFF"
export ARROW_BUILD_UTILITIES="OFF"
/arrow/ci/docker_build_cpp.sh || exit 1
pushd /build/cpp

mkdir ./relwithdebinfo/out
cp ./relwithdebinfo/arrow-ipc-fuzzing-test ./relwithdebinfo/out/fuzzer
ldd ./relwithdebinfo/arrow-ipc-fuzzing-test | grep "=> /" | awk '{print $3}' | xargs -I '{}' cp -v '{}' ./relwithdebinfo/out/.
cd ./relwithdebinfo/out/
tar -czvf fuzzer.tar.gz *
stat fuzzer.tar.gz
cd ../../

export TARGET_ID=apache-arrow/arrow-ipc-fuzzing

wget -O fuzzit https://github.com/fuzzitdev/fuzzit/releases/latest/download/fuzzit_Linux_x86_64
chmod a+x fuzzit

./fuzzit create job --type $FUZZIT_JOB_TYPE --host bionic-llvm7 --revision $CI_ARROW_SHA --branch $CI_ARROW_BRANCH $TARGET_ID ./relwithdebinfo/out/fuzzer.tar.gz
