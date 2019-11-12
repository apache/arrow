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

# install build essentials
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        ca-certificates \
        ccache \
        clang-7 \
        cmake \
        git \
        g++ \
        gcc \
        libboost-all-dev \
        libdouble-conversion-dev \
        libgflags-dev \
        libgoogle-glog-dev \
        libgtest-dev \
        liblz4-dev \
        libre2-dev \
        libsnappy-dev \
        libssl-dev \
        llvm-7-dev \
        make \
        ninja-build \
        pkg-config \
        protobuf-compiler \
        rapidjson-dev \
        thrift-compiler \
        tzdata \
        zlib1g-dev \
        wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ARG cmake=3.11.4
RUN wget -nv -O - https://github.com/Kitware/CMake/releases/download/v${cmake}/cmake-${cmake}-Linux-x86_64.tar.gz | tar -xzf - -C /opt
ENV PATH=/opt/cmake-${cmake}-Linux-x86_64/bin:$PATH

ENV CC=gcc \
    CXX=g++ \
    ORC_SOURCE=BUNDLED \
    Protobuf_SOURCE=BUNDLED \
    ARROW_BUILD_TESTS=OFF \
    ARROW_FLIGHT=OFF \
    ARROW_PARQUET=OFF \
    ARROW_PLASMA=ON \
    ARROW_ORC=ON \
    ARROW_JNI=ON \
    ARROW_GANDIVA=ON \
    ARROW_GANDIVA_JAVA=ON \
    ARROW_HOME=/usr/local
