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

FROM ubuntu:bionic

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends wget software-properties-common gpg-agent && \
    wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    apt-add-repository -y "deb http://apt.llvm.org/bionic/ llvm-toolchain-bionic-7 main" && \
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
        libbenchmark-dev \
        libboost-all-dev \
        libbrotli-dev \
        libbz2-dev \
        libc-ares-dev \
        libdouble-conversion-dev \
        libgflags-dev \
        libgoogle-glog-dev \
        libgrpc-dev \
        libgrpc++-dev \
        liblz4-dev \
        libprotoc-dev \
        libprotobuf-dev \
        libre2-dev \
        libsnappy-dev \
        libssl-dev \
        libzstd-dev \
        llvm-7-dev \
        make \
        ninja-build \
        pkg-config \
        protobuf-compiler \
        protobuf-compiler-grpc \
        rapidjson-dev \
        tzdata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Ubuntu's gtest just provides sources, the compiled version is only available
# from Ubuntu Cosmic on.
# ARROW_GANDIVA_JAVA requires CMake 3.11
# TODO: gRPC is too old on Bionic and c-ares CMake config is not installed thus
#   we need to build both from source.
# protobuf does not come with PHP but grpc needs it to built, thus also
# built Protobuf from source: https://github.com/grpc/grpc/issues/15949
ENV CC=gcc \
     CXX=g++ \
     ARROW_BUILD_BENCHMARKS=ON \
     ARROW_BUILD_TESTS=ON \
     ARROW_DEPENDENCY_SOURCE=SYSTEM \
     ARROW_FLIGHT=ON \
     ARROW_GANDIVA=ON \
     ARROW_GANDIVA_JAVA=OFF \
     ARROW_PARQUET=ON \
     ARROW_HOME=/usr \
     ARROW_WITH_ZSTD=ON \
     CMAKE_ARGS="-DThrift_SOURCE=BUNDLED \
-DFlatbuffers_SOURCE=BUNDLED \
-DGTest_SOURCE=BUNDLED \
-DgRPC_SOURCE=BUNDLED \
-Dc-ares_SOURCE=BUNDLED \
-DORC_SOURCE=BUNDLED \
-DProtobuf_SOURCE=BUNDLED"

# build and test
CMD ["arrow/ci/docker_build_and_test_cpp.sh"]
