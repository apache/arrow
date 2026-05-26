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

ARG FROM=amazonlinux:2023
FROM ${FROM}

ARG DEBUG

RUN \
  quiet=$([ "${DEBUG}" = "yes" ] || echo "--quiet") && \
  dnf update -y ${quiet} && \
  dnf install -y ${quiet} \
    bison \
    boost-devel \
    brotli-devel \
    bzip2-devel \
    c-ares-devel \
    clang \
    cmake \
    curl-devel \
    flex \
    gcc-c++ \
    git \
    gobject-introspection-devel \
    grpc-devel \
    grpc-plugins \
    libxml2-devel \
    libzstd-devel \
    llvm-devel \
    lz4-devel \
    ninja-build \
    make \
    meson \
    openssl-devel \
    pkg-config \
    protobuf-compiler \
    protobuf-devel \
    python3-pip \
    rapidjson-devel \
    re2-devel \
    rpmdevtools \
    snappy-devel \
    tar \
    utf8proc-devel \
    vala \
    which \
    zlib-devel && \
  dnf clean ${quiet} all

ARG cmake=3.26.0
RUN curl -L \
    "https://github.com/Kitware/CMake/releases/download/v${cmake}/cmake-${cmake}-linux-$(uname -m).tar.gz" | \
    tar -xzf - --directory /usr/local --strip-components=1 && \
    ln -fs /usr/local/bin/cmake /usr/bin/cmake
