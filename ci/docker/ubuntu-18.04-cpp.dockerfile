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

ARG base=amd64/ubuntu:18.04
FROM ${base}

# pipefail is enabled for proper error detection in the `wget | apt-key add`
# step
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND=noninteractive

# Installs LLVM toolchain, for Gandiva and testing other compilers
#
# Note that this is installed before the base packages to improve iteration
# while debugging package list with docker build.
ARG clang_tools
ARG llvm
# We can't use LLVM 14 from apt.llvm.org because LLVM 14 requires libgcc-s1
# but libgcc-s1 is available since Ubuntu 20.04.
RUN latest_available_llvm=13 && \
    if [ "${llvm}" -gt "${latest_available_llvm}" ]; then \
      available_llvm="${latest_available_llvm}"; \
    else \
      available_llvm="${llvm}"; \
    fi && \
    if [ "${clang_tools}" -gt "${latest_available_llvm}" ]; then \
      available_clang_tools="${latest_available_llvm}"; \
    else \
      available_clang_tools="${clang_tools}"; \
    fi && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
       apt-transport-https \
       ca-certificates \
       gnupg \
       wget && \
    wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    echo "deb https://apt.llvm.org/bionic/ llvm-toolchain-bionic-${available_llvm} main" > \
       /etc/apt/sources.list.d/llvm.list && \
    if [ "${available_clang_tools}" -ne "${available_llvm}" -a \
         "${available_clang_tools}" -ge 10 ]; then \
      echo "deb https://apt.llvm.org/bionic/ llvm-toolchain-bionic-${available_clang_tools} main" > \
         /etc/apt/sources.list.d/clang-tools.list; \
    fi && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        clang-${available_clang_tools} \
        clang-${available_llvm} \
        clang-format-${available_clang_tools} \
        clang-tidy-${available_clang_tools} \
        llvm-${available_llvm}-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

# Installs C++ toolchain and dependencies
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        autoconf \
        ca-certificates \
        ccache \
        cmake \
        curl \
        g++ \
        gcc \
        gdb \
        git \
        libbenchmark-dev \
        libboost-filesystem-dev \
        libboost-system-dev \
        libbrotli-dev \
        libbz2-dev \
        libc-ares-dev \
        libcurl4-openssl-dev \
        libgflags-dev \
        libgoogle-glog-dev \
        libidn2-dev \
        libkrb5-dev \
        libldap-dev \
        liblz4-dev \
        libnghttp2-dev \
        libprotobuf-dev \
        libprotoc-dev \
        libpsl-dev \
        libre2-dev \
        librtmp-dev \
        libsnappy-dev \
        libssh-dev \
        libssh2-1-dev \
        libssl-dev \
        ninja-build \
        pkg-config \
        protobuf-compiler \
        rapidjson-dev \
        rsync \
        tzdata && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

# Prioritize system packages and local installation
# The following dependencies will be downloaded due to missing/invalid packages
# provided by the distribution:
# - libc-ares-dev does not install CMake config files
# - flatbuffer is not packaged
# - libgtest-dev only provide sources
# - libprotobuf-dev only provide sources
# - thrift is too old
# - utf8proc is too old(v2.1.0)
# - s3 tests would require boost-asio that is included since Boost 1.66.0
# ARROW-17051: this build uses static Protobuf, so we must also use
# static Arrow to run Flight/Flight SQL tests

COPY ci/scripts/install_sccache.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_sccache.sh unknown-linux-musl /usr/local/bin

ENV ARROW_BUILD_STATIC=ON \
    ARROW_BUILD_TESTS=ON \
    ARROW_DATASET=ON \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_FLIGHT=OFF \
    ARROW_GANDIVA=ON \
    ARROW_HDFS=ON \
    ARROW_HOME=/usr/local \
    ARROW_INSTALL_NAME_RPATH=OFF \
    ARROW_NO_DEPRECATED_API=ON \
    ARROW_ORC=ON \
    ARROW_PARQUET=ON \
    ARROW_PLASMA=ON \
    ARROW_USE_ASAN=OFF \
    ARROW_USE_CCACHE=ON \
    ARROW_USE_TSAN=OFF \
    ARROW_USE_UBSAN=OFF \
    ARROW_WITH_BROTLI=ON \
    ARROW_WITH_BZ2=ON \
    ARROW_WITH_LZ4=ON \
    ARROW_WITH_OPENTELEMETRY=OFF \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_ZLIB=ON \
    ARROW_WITH_ZSTD=ON \
    AWSSDK_SOURCE=BUNDLED \
    GTest_SOURCE=BUNDLED \
    ORC_SOURCE=BUNDLED \
    PARQUET_BUILD_EXAMPLES=ON \
    PARQUET_BUILD_EXECUTABLES=ON \
    PATH=/usr/lib/ccache/:$PATH \
    Thrift_SOURCE=BUNDLED \
    utf8proc_SOURCE=BUNDLED \
    xsimd_SOURCE=BUNDLED \
    zstd_SOURCE=BUNDLED
