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

ARG base
FROM ${base}

# pipefail is enabled for proper error detection in the `wget | apt-key add`
# step
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND noninteractive

RUN \
  echo "deb http://deb.debian.org/debian buster-backports main" > \
    /etc/apt/sources.list.d/backports.list

ARG llvm
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
      apt-transport-https \
      lsb-release \
      ca-certificates \
      gnupg \
      software-properties-common \
      wget && \
    wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    echo "deb https://apt.llvm.org/buster/ llvm-toolchain-buster-${llvm} main" > \
        /etc/apt/sources.list.d/llvm.list && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        autoconf \        
        ccache \
        clang-${llvm} \
        cmake \        
        g++ \
        gcc \
        gdb \
        git \    
        libboost-all-dev \
        libgflags-dev \
        libc-ares-dev \
        libbrotli-dev \
        libbz2-dev \
        libcurl4-openssl-dev \
        libutf8proc-dev \
        libgoogle-glog-dev \
        libgtest-dev \
        liblz4-dev \
        libre2-dev \
        libsnappy-dev \
        libssl-dev \
        libthrift-dev \
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

ENV ARROW_BUILD_TESTS=OFF \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_DATASET=ON \
    ARROW_FLIGHT=OFF \
    ARROW_GANDIVA_JAVA=OFF \
    ARROW_GANDIVA=OFF \
    ARROW_HOME=/usr/local \
    ARROW_JNI=OFF \
    ARROW_ORC=OFF \
    ARROW_PARQUET=ON \
    ARROW_PLASMA_JAVA_CLIENT=OFF \
    ARROW_PLASMA=OFF \
    ARROW_USE_CCACHE=ON \
    CC=gcc \
    CXX=g++ \    
    PATH=/usr/lib/ccache/:$PATH \
    Protobuf_SOURCE=BUNDLED
