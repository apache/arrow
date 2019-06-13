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

FROM alpine:3.9

# Install basic build dependencies
# grpc requires libnsl-dev to be present, this cannot be installed via a bundle.
RUN apk add --no-cache -q \
        autoconf \
        bash \
        bison \
        boost-dev \
        ccache \
        cmake \
        flex \
        g++ \
        gcc \
        git \
        gzip \
        make \
        libnsl-dev \
        musl-dev \
        ninja \
        openssl-dev \
        wget \
        zlib-dev

# Ganidva is deactivated as we don't support building LLVM via ExternalProject
# and Alpine only has LLVM 6 in its repositories yet.
# ARROW-4917: ORC fails with compiler problems
ENV CC=gcc \
    CXX=g++ \
    ARROW_FLIGHT=OFF \
    ARROW_GANDIVA=OFF \
    ARROW_ORC=OFF \
    ARROW_PARQUET=ON \
    ARROW_BUILD_TESTS=ON \
    ARROW_HOME=/usr/local

# build and test
CMD ["arrow/ci/docker_build_and_test_cpp.sh"]
