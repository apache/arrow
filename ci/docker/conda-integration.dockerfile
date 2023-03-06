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

ARG repo
ARG arch=amd64
FROM ${repo}:${arch}-conda-cpp

ARG arch=amd64
ARG maven=3.5
ARG node=16
ARG yarn=1.22
ARG jdk=8
ARG go=1.15

# Install Archery and integration dependencies
COPY ci/conda_env_archery.txt /arrow/ci/

RUN mamba install -q -y \
        --file arrow/ci/conda_env_archery.txt \
        "python>=3.7" \
        numpy \
        compilers \
        maven=${maven} \
        nodejs=${node} \
        yarn=${yarn} \
        openjdk=${jdk} && \
    mamba clean --all --force-pkgs-dirs

# Install Rust with only the needed components
# (rustfmt is needed for tonic-build to compile the protobuf definitions)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --profile=minimal -y && \
    $HOME/.cargo/bin/rustup toolchain install stable && \
    $HOME/.cargo/bin/rustup component add rustfmt

ENV GOROOT=/opt/go \
    GOBIN=/opt/go/bin \
    GOPATH=/go \
    PATH=/opt/go/bin:$PATH
RUN wget -nv -O - https://dl.google.com/go/go${go}.linux-${arch}.tar.gz | tar -xzf - -C /opt

ENV DOTNET_ROOT=/opt/dotnet \
    PATH=/opt/dotnet:$PATH
RUN curl -sSL https://dot.net/v1/dotnet-install.sh | bash /dev/stdin -Channel 7.0 -InstallDir /opt/dotnet

ENV ARROW_BUILD_INTEGRATION=ON \
    ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_COMPUTE=OFF \
    ARROW_CSV=OFF \
    ARROW_DATASET=OFF \
    ARROW_FILESYSTEM=OFF \
    ARROW_FLIGHT=ON \
    ARROW_FLIGHT_SQL=ON \
    ARROW_GANDIVA=OFF \
    ARROW_HDFS=OFF \
    ARROW_JEMALLOC=OFF \
    ARROW_JSON=OFF \
    ARROW_ORC=OFF \
    ARROW_PARQUET=OFF \
    ARROW_PLASMA=OFF \
    ARROW_S3=OFF \
    ARROW_USE_GLOG=OFF \
    CMAKE_UNITY_BUILD=ON
