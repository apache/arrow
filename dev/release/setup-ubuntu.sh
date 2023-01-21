#!/bin/bash
#
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

# A script to install dependencies required for release
# verification on Ubuntu.

set -exu

codename=$(. /etc/os-release && echo ${UBUNTU_CODENAME})

case ${codename} in
  bionic)
    llvm=12
    nlohmann_json=
    python=3.8
    apt-get update -y -q
    apt-get install -y -q --no-install-recommends \
      apt-transport-https \
      ca-certificates \
      gnupg \
      wget
    wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    echo "deb https://apt.llvm.org/${codename}/ llvm-toolchain-${codename}-${llvm} main" > \
      /etc/apt/sources.list.d/llvm.list
    apt-get update -y -q
    apt-get install -y -q --no-install-recommends \
      llvm-${llvm}-dev
    ;;
  *)
    nlohmann_json=3
    python=3
    apt-get update -y -q
    apt-get install -y -q --no-install-recommends \
      llvm-dev
    ;;
esac

case ${codename} in
  bionic|focal)
    ;;
  *)
    apt-get update -y -q
    apt-get install -y -q --no-install-recommends \
      libxsimd-dev
    ;;
esac

apt-get install -y -q --no-install-recommends \
  build-essential \
  bundler \
  clang \
  cmake \
  curl \
  git \
  gnupg \
  libcurl4-openssl-dev \
  libgirepository1.0-dev \
  libglib2.0-dev \
  libsqlite3-dev \
  libssl-dev \
  maven \
  ninja-build \
  nlohmann-json${nlohmann_json}-dev \
  openjdk-11-jdk \
  pkg-config \
  python${python}-dev \
  python${python}-venv \
  python3-pip \
  ruby-dev \
  tzdata \
  wget

case ${codename} in
  bionic)
    python${python} -m pip install -U pip
    update-alternatives \
      --install /usr/bin/python3 python3 /usr/bin/python${python} 1
    ;;
esac
