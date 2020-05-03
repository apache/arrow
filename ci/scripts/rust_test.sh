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

export ARROW_TEST_DATA=${arrow_dir}/testing/data
export PARQUET_TEST_DATA=${arrow_dir}/cpp/submodules/parquet-testing/data
export CARGO_TARGET_DIR=${build_dir}
export RUSTFLAGS="-D warnings"

pushd ${source_dir}

# run unit tests
cargo test

# test arrow examples
pushd arrow
cargo run --example builders
cargo run --example dynamic_types
cargo run --example read_csv
cargo run --example read_csv_infer_schema
popd

# test datafusion examples
pushd datafusion
cargo run --example csv_sql
cargo run --example parquet_sql
popd

popd
