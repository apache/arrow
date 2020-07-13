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

ARG base=amd64/ubuntu:14.04
FROM ${base}

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends wget software-properties-common && \
    apt-get install -y -q --no-install-recommends \
        autoconf \
        ca-certificates \
        ccache \
        g++ \
        gcc \
        gdb \
        git \
        libbz2-dev \
        libgoogle-glog-dev \
        libsnappy-dev \
        libssl-dev \
        make \
        ninja-build \
        pkg-config \
        protobuf-compiler \
        python-pip \
        tzdata && \
    apt-get clean && \
    python -m pip install --upgrade pip && \
    pip install --upgrade cmake && \
    rm -rf /var/lib/apt/lists/*

# - Flight is deactivated because the OpenSSL in Ubuntu 14.04 is too old
# - Disabling Gandiva since LLVM 7 require gcc >= 4.9 toolchain
# - ORC has compilation warnings that cause errors on gcc 4.8
# - c-ares in Trusty isn't recognized by gRPC build system
# - libprotobuf-dev / libprotoc-dev in Trusty too old for gRPC
# - libbrotli-dev unavailable
# - libgflags-dev is too old
# - libre2-dev unavailable
# - liblz4-dev is too old
ENV ARROW_BUILD_TESTS=ON \
    ARROW_DATASET=ON \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_FLIGHT=OFF \
    ARROW_GANDIVA_JAVA=OFF \
    ARROW_GANDIVA=OFF \
    ARROW_HOME=/usr/local \
    ARROW_ORC=OFF \
    ARROW_PARQUET=ON \
    ARROW_USE_CCACHE=ON \
    ARROW_WITH_BROTLI=ON \
    ARROW_WITH_BZ2=ON \
    ARROW_WITH_LZ4=ON \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_ZLIB=ON \
    ARROW_WITH_ZSTD=ON \
    benchmark_SOURCE=BUNDLED \
    BOOST_SOURCE=BUNDLED \
    Brotli_SOURCE=BUNDLED \
    cares_SOURCE=BUNDLED \
    CC=gcc \
    CXX=g++ \
    gflags_SOURCE=BUNDLED \
    gRPC_SOURCE=BUNDLED \
    GTest_SOURCE=BUNDLED \
    Lz4_SOURCE=BUNDLED \
    ORC_SOURCE=BUNDLED \
    PARQUET_BUILD_EXAMPLES=ON \
    PARQUET_BUILD_EXECUTABLES=ON \
    PARQUET_REQUIRE_ENCRYPTION=OFF \
    PATH=/usr/lib/ccache/:$PATH \
    Protobuf_SOURCE=BUNDLED \
    RapidJSON_SOURCE=BUNDLED \
    RE2_SOURCE=BUNDLED \
    Thrift_SOURCE=BUNDLED \
    utf8proc_SOURCE=BUNDLED \
    ZSTD_SOURCE=BUNDLED
