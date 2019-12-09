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
FROM ${arch}/ubuntu:16.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends wget software-properties-common && \
    wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    apt-add-repository -y "deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-7 main" && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        autoconf \
        bison \
        ca-certificates \
        ccache \
        clang-7 \
        cmake \
        flex \
        g++ \
        gcc \
        git \
        libboost-all-dev \
        libbrotli-dev \
        libbz2-dev \
        libgoogle-glog-dev \
        liblz4-dev \
        libre2-dev \
        libsnappy-dev \
        libssl-dev \
        llvm-7-dev \
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
# TODO(ARROW-4761): libzstd is too old and external project requires CMake 3.7+
# - c-ares in Xenial isn't recognized by gRPC build system
# - libprotobuf-dev / libprotoc-dev in Xenial too old for gRPC
# - libboost-all-dev does not include Boost.Process, needed for Flight
#   unit tests, so doing vendored build by default
ENV ARROW_BUILD_BENCHMARKS=OFF \
    ARROW_BUILD_TESTS=ON \
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
    ARROW_WITH_ZSTD=OFF \
    BOOST_SOURCE=BUNDLED \
    cares_SOURCE=BUNDLED \
    CC=gcc \
    CXX=g++ \
    gRPC_SOURCE=BUNDLED \
    GTest_SOURCE=BUNDLED \
    ORC_SOURCE=BUNDLED \
    PARQUET_BUILD_EXECUTABLES=ON \
    PATH=/usr/lib/ccache/:$PATH \
    Protobuf_SOURCE=BUNDLED \
    RapidJSON_SOURCE=BUNDLED \
    Thrift_SOURCE=BUNDLED
