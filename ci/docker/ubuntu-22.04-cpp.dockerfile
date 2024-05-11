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

# Installs LLVM toolchain, for Gandiva and testing other compilers
#
# Note that this is installed before the base packages to improve iteration
# while debugging package list with docker build.
ARG clang_tools
ARG llvm
RUN latest_system_llvm=14 && \
    if [ ${llvm} -gt ${latest_system_llvm} -o \
         ${clang_tools} -gt ${latest_system_llvm} ]; then \
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
      fi && \
      if [ ${clang_tools} -ne ${llvm} -a \
           ${clang_tools} -gt ${latest_system_llvm} ]; then \
        echo "deb https://apt.llvm.org/${code_name}/ llvm-toolchain-${code_name}-${clang_tools} main" > \
           /etc/apt/sources.list.d/clang-tools.list; \
      fi; \
    fi && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        clang-${clang_tools} \
        clang-${llvm} \
        clang-format-${clang_tools} \
        clang-tidy-${clang_tools} \
        llvm-${llvm}-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

# Installs C++ toolchain and dependencies
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        autoconf \
        bzip2 \
        ca-certificates \
        ccache \
        cmake \
        curl \
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
        libgmock-dev \
        libgoogle-glog-dev \
        libgrpc++-dev \
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
        libsqlite3-dev \
        libssh-dev \
        libssh2-1-dev \
        libssl-dev \
        libthrift-dev \
        libutf8proc-dev \
        libxml2-dev \
        libzstd-dev \
        make \
        mold \
        ninja-build \
        nlohmann-json3-dev \
        npm \
        pkg-config \
        protobuf-compiler \
        protobuf-compiler-grpc \
        python3-dev \
        python3-pip \
        python3-venv \
        rapidjson-dev \
        rsync \
        tzdata \
        wget \
        xz-utils && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

# install emscripten using EMSDK
ARG emscripten_version="3.1.45"
RUN cd ~ && git clone https://github.com/emscripten-core/emsdk.git && \
    cd emsdk && \
    ./emsdk install ${emscripten_version} && \
    ./emsdk activate ${emscripten_version} && \
    echo "Installed emsdk to:" ~/emsdk


ARG gcc_version=""
RUN if [ "${gcc_version}" = "" ]; then \
      apt-get update -y -q && \
      apt-get install -y -q --no-install-recommends \
          g++ \
          gcc; \
    else \
      if [ "${gcc_version}" -gt "12" ]; then \
          apt-get update -y -q && \
          apt-get install -y -q --no-install-recommends software-properties-common && \
          add-apt-repository ppa:ubuntu-toolchain-r/volatile; \
      fi; \
      apt-get update -y -q && \
      apt-get install -y -q --no-install-recommends \
          g++-${gcc_version} \
          gcc-${gcc_version} && \
      update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-${gcc_version} 100 && \
      update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-${gcc_version} 100 && \
      update-alternatives --install \
        /usr/bin/$(uname --machine)-linux-gnu-gcc \
        $(uname --machine)-linux-gnu-gcc \
        /usr/bin/$(uname --machine)-linux-gnu-gcc-${gcc_version} 100 && \
      update-alternatives --install \
        /usr/bin/$(uname --machine)-linux-gnu-g++ \
        $(uname --machine)-linux-gnu-g++ \
        /usr/bin/$(uname --machine)-linux-gnu-g++-${gcc_version} 100 && \
      update-alternatives --install /usr/bin/cc cc /usr/bin/gcc 100 && \
      update-alternatives --set cc /usr/bin/gcc && \
      update-alternatives --install /usr/bin/c++ c++ /usr/bin/g++ 100 && \
      update-alternatives --set c++ /usr/bin/g++; \
    fi

# make sure zlib is cached in the EMSDK folder
RUN source ~/emsdk/emsdk_env.sh && embuilder --pic build zlib

COPY ci/scripts/install_minio.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_minio.sh latest /usr/local

COPY ci/scripts/install_gcs_testbench.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_gcs_testbench.sh default

COPY ci/scripts/install_azurite.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_azurite.sh

COPY ci/scripts/install_sccache.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_sccache.sh unknown-linux-musl /usr/local/bin

# Prioritize system packages and local installation
# The following dependencies will be downloaded due to missing/invalid packages
# provided by the distribution:
# - Abseil is old
# - libc-ares-dev does not install CMake config files
ENV absl_SOURCE=BUNDLED \
    ARROW_ACERO=ON \
    ARROW_AZURE=ON \
    ARROW_BUILD_STATIC=ON \
    ARROW_BUILD_TESTS=ON \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_DATASET=ON \
    ARROW_FLIGHT=ON \
    ARROW_FLIGHT_SQL=ON \
    ARROW_GANDIVA=ON \
    ARROW_GCS=ON \
    ARROW_HDFS=ON \
    ARROW_HOME=/usr/local \
    ARROW_INSTALL_NAME_RPATH=OFF \
    ARROW_ORC=ON \
    ARROW_PARQUET=ON \
    ARROW_S3=ON \
    ARROW_SUBSTRAIT=ON \
    ARROW_USE_ASAN=OFF \
    ARROW_USE_CCACHE=ON \
    ARROW_USE_MOLD=ON \
    ARROW_USE_UBSAN=OFF \
    ARROW_WITH_BROTLI=ON \
    ARROW_WITH_BZ2=ON \
    ARROW_WITH_LZ4=ON \
    ARROW_WITH_OPENTELEMETRY=ON \
    ARROW_WITH_SNAPPY=ON \
    ARROW_WITH_ZLIB=ON \
    ARROW_WITH_ZSTD=ON \
    ASAN_SYMBOLIZER_PATH=/usr/lib/llvm-${llvm}/bin/llvm-symbolizer \
    AWSSDK_SOURCE=BUNDLED \
    Azure_SOURCE=BUNDLED \
    google_cloud_cpp_storage_SOURCE=BUNDLED \
    ORC_SOURCE=BUNDLED \
    PARQUET_BUILD_EXAMPLES=ON \
    PARQUET_BUILD_EXECUTABLES=ON \
    PATH=/usr/lib/ccache/:$PATH \
    PYTHON=python3 \
    xsimd_SOURCE=BUNDLED
