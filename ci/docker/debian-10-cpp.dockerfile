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
FROM ${arch}/debian:10

ENV DEBIAN_FRONTEND noninteractive

RUN \
  echo "deb http://deb.debian.org/debian buster-backports main" > \
    /etc/apt/sources.list.d/backports.list

ARG llvm
# We can't use LLVM 14 or later from apt.llvm.org on i386 because LLVM
# 14 or later dropped support for i386.
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        dpkg-dev && \
    latest_available_llvm_i386=13 && \
    if [ $(dpkg-architecture -qDEB_HOST_ARCH) = "i386" -a \
         "${llvm}" -gt "${latest_available_llvm_i386}" ]; then \
        available_llvm="${latest_available_llvm_i386}"; \
    else \
        available_llvm="${llvm}"; \
    fi && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        apt-transport-https \
        ca-certificates \
        gnupg \
        wget && \
    wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    echo "deb https://apt.llvm.org/buster/ llvm-toolchain-buster-${available_llvm} main" > \
        /etc/apt/sources.list.d/llvm.list && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        autoconf \
        ccache \
        clang-${available_llvm} \
        cmake \
        curl \
        g++ \
        gcc \
        gdb \
        git \
        libbenchmark-dev \
        libboost-all-dev \
        libbrotli-dev \
        libbz2-dev \
        libc-ares-dev \
        libcurl4-openssl-dev \
        libgflags-dev \
        libgmock-dev \
        libgoogle-glog-dev \
        liblz4-dev \
        libre2-dev \
        libsnappy-dev \
        libssl-dev \
        libthrift-dev \
        libutf8proc-dev \
        llvm-${available_llvm}-dev \
        make \
        ninja-build \
        nlohmann-json3-dev \
        pkg-config \
        protobuf-compiler \
        python3-pip \
        rapidjson-dev \
        rsync \
        tzdata \
        zlib1g-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ci/scripts/install_minio.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_minio.sh latest /usr/local

COPY ci/scripts/install_sccache.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_sccache.sh unknown-linux-musl /usr/local/bin

ENV absl_SOURCE=BUNDLED \
    ARROW_BUILD_TESTS=ON \
    ARROW_DATASET=ON \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_FLIGHT=ON \
    ARROW_GANDIVA=ON \
    ARROW_HOME=/usr/local \
    ARROW_ORC=ON \
    ARROW_PARQUET=ON \
    ARROW_PLASMA=ON \
    ARROW_S3=ON \
    ARROW_USE_CCACHE=ON \
    ARROW_WITH_BROTLI=ON \
    ARROW_WITH_BZ2=ON \
    ARROW_WITH_LZ4=ON \
    ARROW_WITH_OPENTELEMETRY=OFF \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_ZLIB=ON \
    ARROW_WITH_ZSTD=ON \
    AWSSDK_SOURCE=BUNDLED \
    cares_SOURCE=BUNDLED \
    CC=gcc \
    CXX=g++ \
    gRPC_SOURCE=BUNDLED \
    GTest_SOURCE=BUNDLED \
    ORC_SOURCE=BUNDLED \
    PATH=/usr/lib/ccache/:$PATH \
    Protobuf_SOURCE=BUNDLED \
    xsimd_SOURCE=BUNDLED \
    zstd_SOURCE=BUNDLED
