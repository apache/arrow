#!/usr/bin/env bash

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

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh

if [ ! -e $CPP_TOOLCHAIN ]; then
    # Set up C++ toolchain from conda-forge packages for faster builds
    conda create -y -q -p $CPP_TOOLCHAIN python=2.7 \
        nomkl \
        boost-cpp \
        rapidjson \
        flatbuffers \
        gflags \
        gtest \
        lz4-c \
        snappy \
        ccache \
        zstd \
        brotli \
        zlib \
        cmake \
        curl \
        thrift-cpp=0.11.0 \
        ninja

    # HACK(wesm): We started experiencing OpenSSL failures when Miniconda was
    # updated sometime on October 2 or October 3
    conda update -y -q -p $CPP_TOOLCHAIN ca-certificates -c defaults
fi
