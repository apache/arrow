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
FROM ${arch}/ubuntu:18.04

# pipefail is enabled for proper error detection in the `wget | apt-key add`
# step
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        wget \
        software-properties-common \
        gpg-agent && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

# Installs LLVM toolchain, for gandiva and testing other compilers
#
# Note that this is installed before the base packages to improve iteration
# while debugging package list with docker build due to slow download speed of
# llvm compared to ubuntu apt mirrors.
ARG llvm_version=7
ARG llvm_apt_url="http://apt.llvm.org/bionic/"
ARG llvm_apt_arch="llvm-toolchain-bionic-${llvm_version}"
RUN wget -q -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    apt-add-repository -y --update "deb ${llvm_apt_url} ${llvm_apt_arch} main" && \
    apt-get install -y -q --no-install-recommends \
        clang-${llvm_version} \
        clang-format-${llvm_version} \
        clang-tidy-${llvm_version} \
        llvm-${llvm_version}-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

# Installs C++ toolchain and dependencies
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        autoconf \
        bison \
        ca-certificates \
        ccache \
        cmake \
        flex \
        g++ \
        gcc \
        git \
        libbenchmark-dev \
        libboost-filesystem-dev \
        libboost-regex-dev \
        libboost-system-dev \
        libbrotli-dev \
        libbz2-dev \
        libgflags-dev \
        libgoogle-glog-dev \
        liblz4-dev \
        libprotobuf-dev \
        libprotoc-dev \
        libre2-dev \
        libsnappy-dev \
        libssl-dev \
        libzstd-dev \
        ninja-build \
        pkg-config \
        protobuf-compiler \
        rapidjson-dev \
        thrift-compiler \
        tzdata && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

# Prioritize system packages and local installation
# The following dependencies will be downloaded due to missing/invalid packages
# provided by the distribution:
# - libc-ares-dev does not install CMake config files
# - flatbuffer is not packaged
# - libgtest-dev only provide sources
# - libprotobuf-dev only provide sources
# - thrift is too old
ENV ARROW_BUILD_TESTS=ON \
    ARROW_FLIGHT=OFF \
    ARROW_GANDIVA=ON \
    ARROW_HDFS=ON \
    ARROW_HOME=/usr/local \
    ARROW_INSTALL_NAME_RPATH=OFF \
    ARROW_NO_DEPRECATED_API=ON \
    ARROW_ORC=ON \
    ARROW_PARQUET=ON \
    ARROW_PLASMA=ON \
    ARROW_USE_ASAN=OFF \
    ARROW_USE_UBSAN=OFF \
    ARROW_WITH_ZLIB=ON \
    ARROW_WITH_LZ4=ON \
    ARROW_WITH_BZ2=ON \
    ARROW_WITH_ZSTD=ON \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_BROTLI=ON \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ORC_SOURCE=BUNDLED \
    GTest_SOURCE=BUNDLED \
    Thrift_SOURCE=BUNDLED
