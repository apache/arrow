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
# We need to synchronize the following values with the values in .env
# and services.conda-integration in docker-compose.yml.
ARG maven=3.8.7
ARG node=20
ARG yarn=1.22
ARG jdk=17

# Install Archery and integration dependencies
COPY ci/conda_env_archery.txt /arrow/ci/

# Pin Python until pythonnet is made compatible with 3.12
# (https://github.com/pythonnet/pythonnet/pull/2249)
RUN mamba install -q -y \
        --file arrow/ci/conda_env_archery.txt \
        "python < 3.12" \
        numpy \
        compilers \
        maven=${maven} \
        nodejs=${node} \
        yarn=${yarn} \
        openjdk=${jdk} \
        zstd && \
    mamba clean --yes --all --force-pkgs-dirs

# Install Rust with only the needed components
# (rustfmt is needed for tonic-build to compile the protobuf definitions)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --profile=minimal -y && \
    $HOME/.cargo/bin/rustup component add rustfmt

ENV GOROOT=/opt/go \
    GOBIN=/opt/go/bin \
    GOPATH=/go \
    PATH=/opt/go/bin:$PATH
# Use always latest go
RUN wget -nv -O - https://dl.google.com/go/go$( \
        curl \
        --fail \
        --location \
        --show-error \
        --silent \
        https://api.github.com/repos/golang/go/git/matching-refs/tags/go | \
        grep -o '"ref": "refs/tags/go.*"' | \
        tail -n 1 | \
        sed \
        -e 's,^"ref": "refs/tags/go,,g' \
        -e 's/"$//g' \
    ).linux-${arch}.tar.gz | tar -xzf - -C /opt

ENV DOTNET_ROOT=/opt/dotnet \
    PATH=/opt/dotnet:$PATH
RUN curl -sSL https://dot.net/v1/dotnet-install.sh | bash /dev/stdin -Channel 8.0 -InstallDir /opt/dotnet

ENV ARROW_ACERO=OFF \
    ARROW_AZURE=OFF \
    ARROW_BUILD_INTEGRATION=ON \
    ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_COMPUTE=OFF \
    ARROW_CSV=OFF \
    ARROW_DATASET=OFF \
    ARROW_FILESYSTEM=OFF \
    ARROW_FLIGHT=ON \
    ARROW_FLIGHT_SQL=ON \
    ARROW_GANDIVA=OFF \
    ARROW_GCS=OFF \
    ARROW_HDFS=OFF \
    ARROW_JEMALLOC=OFF \
    ARROW_JSON=OFF \
    ARROW_ORC=OFF \
    ARROW_PARQUET=OFF \
    ARROW_S3=OFF \
    ARROW_SUBSTRAIT=OFF \
    ARROW_USE_GLOG=OFF \
    CMAKE_UNITY_BUILD=ON
