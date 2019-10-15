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

cd /arrow/rust

# show activated toolchain
rustup show

# clean first
cargo clean

# raises on any formatting errors
echo "Running formatting checks ..."
cargo +stable fmt --all -- --check
echo "Formatting checks completed"

# build entire project
RUSTFLAGS="-D warnings" cargo build --all-targets

# run tests
cargo test

# make sure we can build Arrow sub-crate without default features
pushd arrow
cargo build --no-default-features
popd

# run Arrow examples
pushd arrow
cargo run --example builders
cargo run --example dynamic_types
cargo run --example read_csv
cargo run --example read_csv_infer_schema
popd

# run DataFusion examples
pushd datafusion
cargo run --example csv_sql
cargo run --example parquet_sql
popd
