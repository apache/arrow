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

FROM ubuntu:18.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -y -q && \
  apt-get install -y -q --no-install-recommends wget software-properties-common gpg-agent && \
  wget --quiet -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
  apt-add-repository -y "deb http://apt.llvm.org/bionic llvm-toolchain-bionic-7 main" && \
  apt-get -y install clang-7

RUN apt update && \
  apt install -y -V \
    autoconf-archive \
    bison \
    cmake \
    flex \
    g++ \
    gcc \
    gtk-doc-tools \
    libboost-filesystem-dev \
    libboost-regex-dev \
    libboost-system-dev \
    libgirepository1.0-dev \
    libglib2.0-doc \
    libprotobuf-dev \
    libprotoc-dev \
    libtool \
    lsb-release \
    make \
    pkg-config \
    protobuf-compiler && \
  apt clean && \
  rm -rf /var/lib/apt/lists/*

COPY build.sh /build.sh
