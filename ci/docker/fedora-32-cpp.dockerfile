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

ARG arch
FROM ${arch}/fedora:32

# install dependencies
RUN dnf update -y && \
	dnf install -y \
        autoconf \
        boost-devel \
        brotli-devel \
        bzip2-devel \
        c-ares-devel \
        ccache \
        clang-devel \
        cmake \
        flatbuffers-devel \
        java-1.8.0-openjdk-devel \
        java-1.8.0-openjdk-headless \
        gcc \
        gcc-c++ \
        glog-devel \
        gflags-devel \
        gmock-devel \
        google-benchmark-devel \
        protobuf-devel \
        gtest-devel \
        git \
        libzstd-devel \
        llvm-devel \
        llvm-static \
        lz4-devel \
        make \
        ninja-build \
        openssl-devel \
        python \
        rapidjson-devel \
        re2-devel \
        snappy-devel \
        thrift-devel \
        utf8proc-devel \
        which \
        zlib-devel

# * gRPC 1.26 in Fedora 32 may have a problem. arrow-flight-test is stuck.
ENV ARROW_BUILD_TESTS=ON \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_DATASET=ON \
    ARROW_FLIGHT=ON \
    ARROW_GANDIVA_JAVA=ON \
    ARROW_GANDIVA=ON \
    ARROW_HOME=/usr/local \
    ARROW_ORC=ON \
    ARROW_PARQUET=ON \
    ARROW_USE_CCACHE=ON \
    ARROW_WITH_BROTLI=ON \
    ARROW_WITH_BZ2=ON \
    ARROW_WITH_LZ4=ON \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_ZLIB=ON \
    ARROW_WITH_ZSTD=ON \
    CC=gcc \
    CXX=g++ \
    gRPC_SOURCE=BUNDLED \
    ORC_SOURCE=BUNDLED \
    PARQUET_BUILD_EXECUTABLES=ON \
    PARQUET_BUILD_EXAMPLES=ON \
    PATH=/usr/lib/ccache/:$PATH
