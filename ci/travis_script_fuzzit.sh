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
export FUZZIT_API_KEY=${FUZZIT_API_KEY:-ac6089a1bc2313679f2d99bb80553162c380676bff3f094de826b16229e28184a8084b86f52c95112bde6b3dbb07b9b7}
wget -O fuzzit https://bin.fuzzit.dev/fuzzit-1.1
chmod a+x fuzzit
./fuzzit auth $FUZZIT_API_KEY

# Because arrow is not build statically we send the shared object together with the binary
mkdir $ARROW_CPP_BUILD_DIR/relwithdebinfo/out
cp $ARROW_CPP_BUILD_DIR/relwithdebinfo/arrow-ipc-fuzzing-test $ARROW_CPP_BUILD_DIR/relwithdebinfo/out/fuzzer
ldd $ARROW_CPP_BUILD_DIR/relwithdebinfo/arrow-ipc-fuzzing-test | grep "=> /" | awk '{print $3}' | xargs -I '{}' cp -v '{}' $ARROW_CPP_BUILD_DIR/relwithdebinfo/out/.
cd $ARROW_CPP_BUILD_DIR/relwithdebinfo/out/
tar -czvf fuzzer.tar.gz *
cd ../../../
./fuzzit create job --type fuzzing --revision $TRAVIS_COMMIT --branch $TRAVIS_BRANCH $TARGET_ID $ARROW_CPP_BUILD_DIR/relwithdebinfo/out/fuzzer.tar.gz

