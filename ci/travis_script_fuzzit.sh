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

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

export TARGET_ID=u79f6bXYgNH4NkU99iWK
wget -O fuzzit https://bin.fuzzit.dev/fuzzit-1.1
chmod a+x fuzzit
./fuzzit auth $FUZZIT_API_KEY

# Because arrow is not build statically we send the shared object together with the binary
mkdir $ARROW_CPP_BUILD_DIR/relwithdebinfo/out
cp $ARROW_CPP_BUILD_DIR/relwithdebinfo/arrow-ipc-fuzzing-test $ARROW_CPP_BUILD_DIR/relwithdebinfo/out/fuzzer
ldd $ARROW_CPP_BUILD_DIR/relwithdebinfo/arrow-ipc-fuzzing-test | grep "=> /" | awk '{print $3}' | xargs -I '{}' cp -v '{}' $ARROW_CPP_BUILD_DIR/relwithdebinfo/out/.
tar -czvf fuzzer.tar.gz $ARROW_CPP_BUILD_DIR/relwithdebinfo/out/*

./fuzzit create job --type sanity --revision $TRAVIS_COMMIT --branch $TRAVIS_BRANCH $TARGET_ID $ARROW_CPP_BUILD_DIR/relwithdebinfo/arrow-ipc-fuzzing-test


