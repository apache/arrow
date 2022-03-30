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
# verification on Ubuntu 20.04

apt-get update
apt-get -y install \
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
  llvm-dev \
  maven \
  ninja-build \
  openjdk-11-jdk \
  pkg-config \
  python3-pip \
  python3-venv \
  ruby-dev \
  wget
