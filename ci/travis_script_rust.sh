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

RUST_DIR=${TRAVIS_BUILD_DIR}/rust

pushd $RUST_DIR

# show activated toolchain
rustup show

# check code formatting only for Rust nightly
if [ $RUSTUP_TOOLCHAIN == "nightly" ]
then
  # raises on any formatting errors
  rustup component add rustfmt-preview
  cargo fmt --all -- --check
fi

# raises on any warnings
cargo rustc -- -D warnings

cargo build
cargo test
cargo bench
cargo run --example builders
cargo run --example dynamic_types
cargo run --example read_csv

popd
