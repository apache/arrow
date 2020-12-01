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

ARG rust=nightly-2020-11-24

# freeze the version for deterministic builds
RUN rustup default ${rust} && \
    rustup component add clippy rustfmt --toolchain ${rust}-x86_64-unknown-linux-gnu && \
    rustup toolchain add stable-x86_64-unknown-linux-gnu
