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

FROM wch1/r-debug:latest

# Installs C++ toolchain and dependencies
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
      autoconf \
      ca-certificates \
      ccache \
      cmake \
      g++ \
      gcc \
      git \
      libbenchmark-dev \
      libboost-filesystem-dev \
      libboost-regex-dev \
      libboost-system-dev \
      libbrotli-dev \
      libbz2-dev \
      libgflags-dev \
      libgoogle-glog-dev \
      liblz4-dev \
      liblzma-dev \
      libre2-dev \
      libsnappy-dev \
      libssl-dev \
      libzstd-dev \
      locales \
      ninja-build \
      pkg-config \
      rapidjson-dev \
      tzdata && \
      locale-gen en_US.UTF-8 && \
      apt-get clean && rm -rf /var/lib/apt/lists*

# Ensure parallel R package installation, set CRAN repo mirror,
# and use pre-built binaries where possible
RUN printf "\
    options(Ncpus = parallel::detectCores(), \
            repos = 'https://packagemanager.rstudio.com/cran/__linux__/bionic/latest', \
            HTTPUserAgent = sprintf(\
                'R/%%s R (%%s)', getRversion(), \
                paste(getRversion(), R.version\$platform, R.version\$arch, R.version\$os)))\n" \
    >> /usr/local/RDsan/lib/R/etc/Rprofile.site

# Also ensure parallel compilation of C/C++ code
RUN echo "MAKEFLAGS=-j$(R -s -e 'cat(parallel::detectCores())')" >> /usr/local/RDsan/lib/R/etc/Makeconf

# Prioritize system packages and local installation
# The following dependencies will be downloaded due to missing/invalid packages
# provided by the distribution:
# - libc-ares-dev does not install CMake config files
# - flatbuffer is not packaged
# - libgtest-dev only provide sources
# - libprotobuf-dev only provide sources
# - thrift is too old
ENV ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_FLIGHT=OFF \
    ARROW_GANDIVA=OFF \
    ARROW_HOME=/usr/local \
    ARROW_HDFS=OFF \
    ARROW_INSTALL_NAME_RPATH=OFF \
    ARROW_NO_DEPRECATED_API=ON \
    ARROW_ORC=OFF \
    ARROW_PARQUET=ON \
    ARROW_PLASMA=OFF \
    ARROW_USE_ASAN=OFF \
    ARROW_USE_CCACHE=ON \
    ARROW_USE_UBSAN=OFF \
    ARROW_WITH_BZ2=OFF \
    ARROW_WITH_UTF8PROC=OFF \
    ARROW_WITH_ZSTD=OFF \
    cares_SOURCE=BUNDLED \
    gRPC_SOURCE=BUNDLED \
    GTest_SOURCE=BUNDLED \
    LC_ALL=en_US.UTF-8 \
    PATH=/usr/lib/ccache/:$PATH \
    Protobuf_SOURCE=BUNDLED \
    R_BIN=RDsan \
    Thrift_SOURCE=BUNDLED

COPY ci/scripts/r_deps.sh /arrow/ci/scripts/
COPY r/DESCRIPTION /arrow/r/
RUN /arrow/ci/scripts/r_deps.sh /arrow
