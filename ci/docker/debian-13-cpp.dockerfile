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
FROM ${arch}/debian:13
ARG arch

ENV DEBIAN_FRONTEND noninteractive

ARG llvm
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        apt-transport-https \
        ca-certificates \
        gnupg \
        lsb-release \
        wget && \
    if [ ${llvm} -ge 20 ]; then \
      wget -O /usr/share/keyrings/llvm-snapshot.asc \
        https://apt.llvm.org/llvm-snapshot.gpg.key && \
      (echo "Types: deb"; \
       echo "URIs: https://apt.llvm.org/$(lsb_release --codename --short)/"; \
       echo "Suites: llvm-toolchain-$(lsb_release --codename --short)-${llvm}"; \
       echo "Components: main"; \
       echo "Signed-By: /usr/share/keyrings/llvm-snapshot.asc") | \
        tee /etc/apt/sources.list.d/llvm.sources; \
    fi && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        autoconf \
        ccache \
        clang-${llvm} \
        cmake \
        curl \
        g++ \
        gcc \
        gdb \
        git \
        libbenchmark-dev \
        libboost-filesystem-dev \
        libboost-system-dev \
        libbrotli-dev \
        libbz2-dev \
        libc-ares-dev \
        libcurl4-openssl-dev \
        libgflags-dev \
        libgmock-dev \
        libgoogle-glog-dev \
        libgrpc++-dev \
        libidn2-dev \
        libkrb5-dev \
        libldap-dev \
        liblz4-dev \
        libnghttp2-dev \
        libprotobuf-dev \
        libprotoc-dev \
        libpsl-dev \
        libre2-dev \
        librtmp-dev \
        libsnappy-dev \
        libsqlite3-dev \
        libssh-dev \
        libssh2-1-dev \
        libssl-dev \
        libthrift-dev \
        libutf8proc-dev \
        libxml2-dev \
        libxsimd-dev \
        libzstd-dev \
        llvm-${llvm}-dev \
        make \
        ninja-build \
        nlohmann-json3-dev \
        npm \
        opentelemetry-cpp-dev \
        pkg-config \
        protobuf-compiler-grpc \
        python3-dev \
        python3-pip \
        python3-venv \
        rapidjson-dev \
        rsync \
        tzdata \
        zlib1g-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ci/scripts/install_minio.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_minio.sh latest /usr/local

COPY ci/scripts/install_gcs_testbench.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_gcs_testbench.sh default

COPY ci/scripts/install_azurite.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_azurite.sh

COPY ci/scripts/install_sccache.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_sccache.sh unknown-linux-musl /usr/local/bin

# Prioritize system packages and local installation.
ENV ARROW_ACERO=ON \
    ARROW_AZURE=ON \
    ARROW_BUILD_TESTS=ON \
    ARROW_DATASET=ON \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_DATASET=ON \
    ARROW_FLIGHT=ON \
    ARROW_FLIGHT_SQL=ON \
    ARROW_GANDIVA=ON \
    ARROW_GCS=ON \
    ARROW_HOME=/usr/local \
    ARROW_JEMALLOC=ON \
    ARROW_ORC=ON \
    ARROW_PARQUET=ON \
    ARROW_S3=ON \
    ARROW_SUBSTRAIT=ON \
    ARROW_USE_CCACHE=ON \
    ARROW_WITH_BROTLI=ON \
    ARROW_WITH_BZ2=ON \
    ARROW_WITH_LZ4=ON \
    ARROW_WITH_OPENTELEMETRY=ON \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_ZLIB=ON \
    ARROW_WITH_ZSTD=ON \
    AWSSDK_SOURCE=BUNDLED \
    Azure_SOURCE=BUNDLED \
    google_cloud_cpp_storage_SOURCE=BUNDLED \
    ORC_SOURCE=BUNDLED \
    PATH=/usr/lib/ccache/:$PATH \
    PYTHON=python3
