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

ARG base=amd64/ubuntu:16.04
FROM ${base}

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND noninteractive

# LLVM 10 or later requires C++ 14 but g++-5's C++ 14 support is limited.
# cpp/src/arrow/vendored/datetime/date.h doesn't work.
# ARG llvm
ENV llvm=8
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        apt-transport-https \
        software-properties-common \
        wget && \
    wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    apt-add-repository -y "deb https://apt.llvm.org/xenial/ llvm-toolchain-xenial-${llvm} main" && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        autoconf \
        ca-certificates \
        ccache \
        clang-${llvm} \
        cmake \
        g++ \
        gcc \
        gdb \
        git \
        libboost-all-dev \
        libbrotli-dev \
        libbz2-dev \
        libgoogle-glog-dev \
        liblz4-dev \
        libre2-dev \
        libssl-dev \
        libutf8proc-dev \
        libzstd1-dev \
        llvm-${llvm}-dev \
        make \
        ninja-build \
        pkg-config \
        protobuf-compiler \
        python3 \
        tzdata && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Benchmark is deactivated as the external project requires CMake 3.6+
# Gandiva JNI is deactivated as it requires CMake 3.11+
# - c-ares in Xenial isn't recognized by gRPC build system
# - libprotobuf-dev / libprotoc-dev in Xenial too old for gRPC
# - libboost-all-dev does not include Boost.Process, needed for Flight
#   unit tests, so doing vendored build by default
ENV ARROW_BUILD_BENCHMARKS=OFF \
    ARROW_BUILD_TESTS=ON \
    ARROW_DATASET=ON \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_GANDIVA_JAVA=OFF \
    ARROW_GANDIVA=ON \
    ARROW_HOME=/usr/local \
    ARROW_PARQUET=ON \
    ARROW_USE_CCACHE=ON \
    ARROW_WITH_BROTLI=ON \
    ARROW_WITH_BZ2=ON \
    ARROW_WITH_LZ4=ON \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_ZLIB=ON \
    ARROW_WITH_ZSTD=ON \
    BOOST_SOURCE=BUNDLED \
    cares_SOURCE=BUNDLED \
    CC=gcc \
    CXX=g++ \
    gRPC_SOURCE=BUNDLED \
    GTest_SOURCE=BUNDLED \
    ORC_SOURCE=BUNDLED \
    PARQUET_BUILD_EXAMPLES=ON \
    PARQUET_BUILD_EXECUTABLES=ON \
    PATH=/usr/lib/ccache/:$PATH \
    Protobuf_SOURCE=BUNDLED \
    RapidJSON_SOURCE=BUNDLED \
    Snappy_SOURCE=BUNDLED \
    Thrift_SOURCE=BUNDLED
