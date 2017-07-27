#!/usr/bin/env bash
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

set -e

export PATH="$MINICONDA/bin:$PATH"
conda update -y -q conda
conda config --set auto_update_conda false
conda info -a

conda config --set show_channel_urls True

# Help with SSL timeouts to S3
conda config --set remote_connect_timeout_secs 12

conda config --add channels https://repo.continuum.io/pkgs/free
conda config --add channels conda-forge
conda info -a

# faster builds, please
conda install -y nomkl

conda install --y conda-build jinja2 anaconda-client cmake curl

# Set up C++ toolchain
conda create -y -q -p $CPP_TOOLCHAIN python=3.6 \
    jemalloc=4.4.0 \
    nomkl \
    boost-cpp \
    rapidjson \
    flatbuffers \
    gflags \
    lz4-c \
    snappy \
    zstd \
    brotli \
    zlib \
    git \
    cmake \
    curl \
    thrift-cpp \
    libhdfs3 \
    ninja

if [ $BUILD_OS_NAME == "osx" ]; then
  brew update > /dev/null
  brew install jemalloc
  brew install ccache
fi
