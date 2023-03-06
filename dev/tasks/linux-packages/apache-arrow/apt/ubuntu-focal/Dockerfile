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

ARG FROM=ubuntu:focal
FROM ${FROM}

RUN \
  echo "debconf debconf/frontend select Noninteractive" | \
    debconf-set-selections

RUN \
  echo 'APT::Install-Recommends "false";' > \
    /etc/apt/apt.conf.d/disable-install-recommends

ARG DEBUG
RUN \
  quiet=$([ "${DEBUG}" = "yes" ] || echo "-qq") && \
  apt update ${quiet} && \
  apt install -y -V ${quiet} \
    build-essential \
    clang \
    cmake \
    debhelper \
    devscripts \
    git \
    gtk-doc-tools \
    libboost-filesystem-dev \
    libboost-system-dev \
    libbrotli-dev \
    libbz2-dev \
    libc-ares-dev \
    libcurl4-openssl-dev \
    libgirepository1.0-dev \
    libglib2.0-doc \
    libgmock-dev \
    libgoogle-glog-dev \
    libgtest-dev \
    liblz4-dev \
    libprotobuf-dev \
    libprotoc-dev \
    libre2-dev \
    libsnappy-dev \
    libssl-dev \
    libthrift-dev \
    libutf8proc-dev \
    libzstd-dev \
    llvm-dev \
    lsb-release \
    ninja-build \
    nlohmann-json3-dev \
    pkg-config \
    protobuf-compiler \
    python3-dev \
    python3-pip \
    python3-setuptools \
    rapidjson-dev \
    tzdata \
    valac \
    zlib1g-dev && \
  if apt list | grep '^nvidia-cuda-toolkit/'; then \
    apt install -y -V ${quiet} nvidia-cuda-toolkit; \
  fi && \
  apt clean && \
  python3 -m pip install --no-use-pep517 meson && \
  ln -s /usr/local/bin/meson /usr/bin/ && \
  rm -rf /var/lib/apt/lists/*
