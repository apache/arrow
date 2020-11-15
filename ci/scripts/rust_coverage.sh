#!/usr/bin/env bash
#
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

arrow_dir=${1}
source_dir=${1}/rust
build_dir=${2}/rust
rust=${3}

export ARROW_TEST_DATA=${arrow_dir}/testing/data
export PARQUET_TEST_DATA=${arrow_dir}/cpp/submodules/parquet-testing/data
export CARGO_TARGET_DIR=${build_dir}

pushd ${source_dir}

rustup default ${rust}
rustup component add rustfmt --toolchain ${rust}-x86_64-unknown-linux-gnu
# 2020-11-15: There is a cargo-tarpaulin regression in 0.17.0
# see https://github.com/xd009642/tarpaulin/issues/618
cargo install --version 0.16.0 cargo-tarpaulin

cargo tarpaulin --out Xml

popd
