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

# ensure that both toolchains are installed
rustup install stable
rustup install nightly

pip install 'travis-cargo<0.2' --user

export PATH=$HOME/.local/bin:$PATH
export CARGO_TARGET_DIR=$TRAVIS_BUILD_DIR/target

cargo install cargo-travis || echo "Skipping cargo-travis installation as it already exists in cache"
export PATH=$HOME/.cargo/bin:$PATH
