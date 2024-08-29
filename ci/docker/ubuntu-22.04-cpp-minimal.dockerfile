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

ARG base=amd64/ubuntu:22.04
FROM ${base}

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN echo "debconf debconf/frontend select Noninteractive" | \
        debconf-set-selections

RUN apt-get update -y -q && \
    apt-get install -y -q \
        build-essential \
        ccache \
        cmake \
        curl \
        git \
        libssl-dev \
        libcurl4-openssl-dev \
        python3-pip \
        tzdata \
        wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

# Installs LLVM toolchain, for Gandiva and testing other compilers
#
# Note that this is installed before the base packages to improve iteration
# while debugging package list with docker build.
ARG llvm
RUN latest_system_llvm=14 && \
    if [ ${llvm} -gt ${latest_system_llvm} ]; then \
      apt-get update -y -q && \
      apt-get install -y -q --no-install-recommends \
          apt-transport-https \
          ca-certificates \
          gnupg \
          lsb-release \
          wget && \
      wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
      code_name=$(lsb_release --codename --short) && \
      if [ ${llvm} -gt 10 ]; then \
        echo "deb https://apt.llvm.org/${code_name}/ llvm-toolchain-${code_name}-${llvm} main" > \
           /etc/apt/sources.list.d/llvm.list; \
      fi; \
    fi && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        clang-${llvm} \
        llvm-${llvm}-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

COPY ci/scripts/install_minio.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_minio.sh latest /usr/local

COPY ci/scripts/install_gcs_testbench.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_gcs_testbench.sh default

COPY ci/scripts/install_sccache.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_sccache.sh unknown-linux-musl /usr/local/bin

ENV ARROW_ACERO=ON \
    ARROW_BUILD_TESTS=ON \
    ARROW_DATASET=ON \
    ARROW_FLIGHT=ON \
    ARROW_GANDIVA=ON \
    ARROW_GCS=ON \
    ARROW_HDFS=ON \
    ARROW_HOME=/usr/local \
    ARROW_INSTALL_NAME_RPATH=OFF \
    ARROW_NO_DEPRECATED_API=ON \
    ARROW_ORC=ON \
    ARROW_PARQUET=ON \
    ARROW_S3=ON \
    ARROW_USE_CCACHE=ON \
    ARROW_WITH_BROTLI=ON \
    ARROW_WITH_BZ2=ON \
    ARROW_WITH_LZ4=ON \
    ARROW_WITH_OPENTELEMETRY=OFF \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_ZLIB=ON \
    ARROW_WITH_ZSTD=ON \
    CMAKE_GENERATOR="Unix Makefiles" \
    PARQUET_BUILD_EXAMPLES=ON \
    PARQUET_BUILD_EXECUTABLES=ON \
    PATH=/usr/lib/ccache/:$PATH \
    PYTHON=python3
