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

ARG FROM=ubuntu:bionic
FROM ${FROM}

COPY qemu-* /usr/bin/

RUN \
  echo "debconf debconf/frontend select Noninteractive" | \
    debconf-set-selections

RUN \
  echo 'APT::Install-Recommends "false";' > \
    /etc/apt/apt.conf.d/disable-install-recommends

ARG DEBUG
ARG LLVM
RUN \
  quiet=$([ "${DEBUG}" = "yes" ] || echo "-qq") && \
  apt update ${quiet} && \
  apt install -y -V ${quiet} \
    apt-transport-https \
    ca-certificates \
    gnupg \
    wget && \
  wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
  echo "deb https://apt.llvm.org/bionic/ llvm-toolchain-bionic-${LLVM} main" > \
    /etc/apt/sources.list.d/llvm.list && \
  apt update ${quiet} && \
  apt install -y -V ${quiet} \
    build-essential \
    cmake \
    devscripts \
    fakeroot \
    git \
    gtk-doc-tools \
    libboost-filesystem-dev \
    libboost-regex-dev \
    libboost-system-dev \
    libbrotli-dev \
    libbz2-dev \
    libgirepository1.0-dev \
    libglib2.0-doc \
    libgoogle-glog-dev \
    libgtest-dev \
    liblz4-dev \
    libre2-dev \
    libsnappy-dev \
    libssl-dev \
    libutf8proc-dev \
    libzstd-dev \
    lsb-release \
    ninja-build \
    pkg-config \
    python3-dev \
    python3-numpy \
    python3-pip \
    python3-setuptools \
    python3-wheel \
    rapidjson-dev \
    tzdata \
    zlib1g-dev && \
  if [ "$(dpkg --print-architecture)" != "arm64" ]; then \
    apt install -y -V ${quiet} \
      clang-${LLVM} \
      llvm-${LLVM}-dev; \
  fi && \
  if apt list | grep '^nvidia-cuda-toolkit/'; then \
    apt install -y -V ${quiet} nvidia-cuda-toolkit; \
  fi && \
  apt install -y -V -t bionic-backports ${quiet} \
    debhelper && \
  pip3 install --upgrade meson && \
  ln -s /usr/local/bin/meson /usr/bin/ && \
  apt clean && \
  rm -rf /var/lib/apt/lists/*
