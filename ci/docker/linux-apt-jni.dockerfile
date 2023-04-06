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
ARG jdk=8
ARG maven=3.5.4
FROM ${arch}/maven:${maven}-jdk-${jdk}

# pipefail is enabled for proper error detection in the `wget | apt-key add`
# step
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND noninteractive

ARG llvm
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
      apt-transport-https \
      lsb-release \
      software-properties-common \
      wget && \
    code_name=$(lsb_release --codename --short) && \
    wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    apt-add-repository -y \
      "deb https://apt.llvm.org/${code_name}/ llvm-toolchain-${code_name}-${llvm} main" && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        ca-certificates \
        ccache \
        clang-${llvm} \
        cmake \
        git \
        g++ \
        gcc \
        libboost-all-dev \
        libcurl4-openssl-dev \
        libgflags-dev \
        libgoogle-glog-dev \
        libgtest-dev \
        liblz4-dev \
        libre2-dev \
        libsnappy-dev \
        libssl-dev \
        llvm-${llvm}-dev \
        make \
        ninja-build \
        pkg-config \
        protobuf-compiler \
        rapidjson-dev \
        tzdata \
        zlib1g-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ARG cmake=3.11.4
RUN wget -nv -O - https://github.com/Kitware/CMake/releases/download/v${cmake}/cmake-${cmake}-Linux-x86_64.tar.gz | tar -xzf - -C /opt
ENV PATH=/opt/cmake-${cmake}-Linux-x86_64/bin:$PATH

ENV ARROW_ACERO=ON \
    ARROW_BUILD_TESTS=ON \
    ARROW_DATASET=ON \
    ARROW_FLIGHT=OFF \
    ARROW_GANDIVA=ON \
    ARROW_HOME=/usr/local \
    ARROW_JAVA_CDATA=ON \
    ARROW_JAVA_JNI=ON \
    ARROW_ORC=ON \
    ARROW_PARQUET=ON \
    ARROW_S3=ON \
    ARROW_USE_CCACHE=ON \
    CC=gcc \
    CXX=g++ \
    ORC_SOURCE=BUNDLED \
    PATH=/usr/lib/ccache/:$PATH \
    Protobuf_SOURCE=BUNDLED
