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
    make install && \
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

# The artifact of the steps below is a "/target" containing compiled dependencies
# Uses the directory /dependency_build for temporary files

# arrow-flight
COPY rust/arrow-flight/Cargo.toml /dependency_build/arrow-flight/Cargo.toml
## Create the directory and place an empty lib.rs (required for steps below)
RUN mkdir -p /dependency_build/arrow-flight/src && touch dependency_build/arrow-flight/src/lib.rs

# arrow
COPY rust/arrow/Cargo.toml /dependency_build/arrow/Cargo.toml
## this is unfortunately necessary as cargo checks that the benches exist
COPY rust/arrow/benches /dependency_build/arrow/benches
RUN mkdir -p /dependency_build/arrow/src && touch /dependency_build/arrow/src/lib.rs

# parquet
COPY rust/parquet/Cargo.toml /dependency_build/parquet/Cargo.toml
COPY rust/parquet/build.rs /dependency_build/parquet/build.rs
RUN mkdir -p /dependency_build/parquet/src && touch /dependency_build/parquet/src/lib.rs

# datafusion
COPY rust/datafusion/Cargo.toml /dependency_build/datafusion/Cargo.toml
COPY rust/datafusion/benches /dependency_build/datafusion/benches
RUN mkdir -p /dependency_build/datafusion/src && touch /dependency_build/datafusion/src/lib.rs

# we can add more workspaces here if they justify the burden vs compile time.

# set configurations valid across builds.
# Changing them requires re-building dependencies
ENV CARGO_HOME="/build/cargo"
ENV RUSTFLAGS="-D warnings"

# Finally, compile parquet's dependencies (will also compile other's dependencies)
RUN cd /dependency_build/datafusion && cargo build --lib

# move the "target" directory to the root, which is our cache, and remove temp directory
RUN mv dependency_build/datafusion/target / && \
    rm -rf /dependency_build
