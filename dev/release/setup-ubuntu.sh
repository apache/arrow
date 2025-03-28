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

version=$(. /etc/os-release && echo ${VERSION_ID})

apt-get update -y -q

if [ ${version} \> "22.04" ]; then
  # Some tests rely on legacy timezone aliases such as "US/Pacific"
  apt-get install -y -q --no-install-recommends \
    tzdata-legacy
fi

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
  libxsimd-dev \
  llvm-dev \
  ninja-build \
  nlohmann-json3-dev \
  pkg-config \
  python3-dev \
  python3-venv \
  python3-pip \
  ruby-dev \
  tzdata \
  wget
