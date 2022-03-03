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

ENV DEBIAN_FRONTEND=noninteractive

ARG llvm=12
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
       apt-transport-https \
       ca-certificates \
       gnupg \
       wget && \
    wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    echo "deb https://apt.llvm.org/bionic/ llvm-toolchain-bionic-${llvm} main" > \
       /etc/apt/sources.list.d/llvm.list && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        build-essential \
        clang \
        cmake \
        curl \
        git \
        libcurl4-openssl-dev \
        libgirepository1.0-dev \
        libglib2.0-dev \
        libsqlite3-dev \
        libssl-dev \
        llvm-${llvm}-dev \
        maven \
        ninja-build \
        openjdk-11-jdk \
        pkg-config \
        python3-pip \
        python3.8-dev \
        python3.8-venv \
        ruby-dev \
        wget \
        tzdata && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

RUN python3.8 -m pip install -U pip && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1
