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
FROM ${arch}/fedora:33
ARG arch

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
        curl-devel \
        flatbuffers-devel \
        gcc \
        gcc-c++ \
        gflags-devel \
        git \
        glog-devel \
        gmock-devel \
        google-benchmark-devel \
        grpc-devel \
        grpc-plugins \
        gtest-devel \
        java-latest-openjdk-devel \
        java-latest-openjdk-headless \
        libzstd-devel \
        llvm-devel \
        llvm-static \
        lz4-devel \
        make \
        ninja-build \
        openssl-devel \
        protobuf-devel \
        python \
        python-pip \
        rapidjson-devel \
        re2-devel \
        snappy-devel \
        thrift-devel \
        utf8proc-devel \
        wget \
        which \
        zlib-devel

COPY ci/scripts/install_minio.sh \
     ci/scripts/install_gcs_testbench.sh \
     /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_minio.sh ${arch} linux latest /usr/local
RUN /arrow/ci/scripts/install_gcs_testbench.sh default

ENV ARROW_BUILD_TESTS=ON \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_DATASET=ON \
    ARROW_FLIGHT=ON \
    ARROW_GANDIVA_JAVA=ON \
    ARROW_GANDIVA=ON \
    ARROW_HOME=/usr/local \
    ARROW_ORC=ON \
    ARROW_PARQUET=ON \
    ARROW_S3=ON \
    ARROW_USE_CCACHE=ON \
    ARROW_WITH_BROTLI=ON \
    ARROW_WITH_BZ2=ON \
    ARROW_WITH_LZ4=ON \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_ZLIB=ON \
    ARROW_WITH_ZSTD=ON \
    AWSSDK_SOURCE=BUNDLED \
    CC=gcc \
    CXX=g++ \
    ORC_SOURCE=BUNDLED \
    PARQUET_BUILD_EXECUTABLES=ON \
    PARQUET_BUILD_EXAMPLES=ON \
    PATH=/usr/lib/ccache/:$PATH
