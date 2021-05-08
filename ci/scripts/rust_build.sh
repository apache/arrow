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

set -e

arrow_dir=${1}
source_dir=${1}/rust

# This file is used to build the rust binaries needed for the archery
# integration tests. Testing of the rust implementation in normal CI is handled
# by github workflows in the arrow-rs repository.

# Disable full debug symbol generation to speed up CI build / reduce memory required
export RUSTFLAGS="-C debuginfo=1"

export ARROW_TEST_DATA=${arrow_dir}/testing/data
export PARQUET_TEST_DATA=${arrow_dir}/cpp/submodules/parquet-testing/data

if [ "${ARCHERY_INTEGRATION_WITH_RUST}" -eq "0" ]; then
  echo "====================================================================="
  echo "Not building the Rust implementation."
  echo "====================================================================="
  exit 0;
elif [ ! -d "${source_dir}" ]; then
  echo "====================================================================="
  echo "The Rust source is missing. Please clone the arrow-rs repository"
  echo "to arrow/rust before running the integration tests:"
  echo "  git clone https://github.com/apache/arrow-rs.git path/to/arrow/rust"
  echo "====================================================================="
  exit 1;
fi

set -x

# show activated toolchain
rustup show

pushd ${source_dir}

# build only the integration testing binaries
cargo build -p arrow-integration-testing

popd
