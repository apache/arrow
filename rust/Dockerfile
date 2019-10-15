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

FROM rust:1.38.0-buster
RUN rustup show

# Install stable rustfmt
RUN rustup install stable
RUN rustup component add rustfmt

# Install the specific version of Rust nightly that we need so that we don't need to download it for every CI run
COPY rust/rust-toolchain /tmp
RUN rustup install "$(cat /tmp/rust-toolchain)"
RUN rustup default "$(cat /tmp/rust-toolchain)"

# Enable stable rustfmt for nightly Rust
RUN rustup component add rustfmt --toolchain stable-x86_64-unknown-linux-gnu

# Set environment variables for location of test data required by unit and integration tests
ENV ARROW_TEST_DATA=/arrow/testing/data
ENV PARQUET_TEST_DATA=/arrow/cpp/submodules/parquet-testing/data

CMD ["/bin/bash", "-c", "arrow/ci/docker_build_rust.sh"]
