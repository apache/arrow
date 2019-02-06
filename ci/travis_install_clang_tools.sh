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


set -ex

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key|sudo apt-key add -
sudo apt-add-repository -y \
     "deb https://apt.llvm.org/$DISTRO_CODENAME/ llvm-toolchain-$DISTRO_CODENAME-$ARROW_LLVM_MAJOR_VERSION main"
sudo apt-get update -qq
sudo apt-get install -q clang-$ARROW_LLVM_MAJOR_VERSION clang-format-$ARROW_LLVM_MAJOR_VERSION clang-tidy-$ARROW_LLVM_MAJOR_VERSION
