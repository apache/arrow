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

ARG arch=amd64
FROM ${arch}/rust

# install pre-requisites for building flatbuffers
RUN apt-get update -y && \
    apt-get install -y build-essential cmake && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# install flatbuffers
ARG flatbuffers=1.11.0
RUN wget -q -O - https://github.com/google/flatbuffers/archive/v${flatbuffers}.tar.gz | tar -xzf - && \
    cd flatbuffers-${flatbuffers} && \
    cmake -G "Unix Makefiles" && \
    make install -j4 && \
    cd / && \
    rm -rf flatbuffers-${flatbuffers}

ARG rust=nightly-2020-04-22

# freeze the version for deterministic builds
RUN rustup default ${rust} && \
    rustup component add rustfmt --toolchain ${rust}-x86_64-unknown-linux-gnu

# Compile a dummy program, so that the dependencies are compiled and cached on a layer
# see https://stackoverflow.com/a/58474618/931303 for details
# We do not compile any of the workspace or we defeat the purpose of caching - we only
# compile their external dependencies.

ENV CARGO_HOME="/rust/cargo" \
    CARGO_TARGET_DIR="/rust/target" \
    RUSTFLAGS="-D warnings"

# The artifact of the steps below is a "${CARGO_TARGET_DIR}" containing
# compiled dependencies. Create the directories and place an empty lib.rs
# files.
COPY rust /arrow/rust
RUN mkdir \
        /arrow/rust/arrow-flight/src \
        /arrow/rust/arrow/src \
        /arrow/rust/benchmarks/src \
        /arrow/rust/datafusion/src \
        /arrow/rust/integration-testing/src  \
        /arrow/rust/parquet/src && \
    touch \
        /arrow/rust/arrow-flight/src/lib.rs \
        /arrow/rust/arrow/src/lib.rs \
        /arrow/rust/benchmarks/src/lib.rs \
        /arrow/rust/datafusion/src/lib.rs \
        /arrow/rust/integration-testing/src/lib.rs  \
        /arrow/rust/parquet/src/lib.rs

# Compile dependencies for the whole workspace
RUN cd /arrow/rust && cargo build --workspace --lib --all-features
