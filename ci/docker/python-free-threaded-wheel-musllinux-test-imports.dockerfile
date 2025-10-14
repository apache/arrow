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

ARG base
FROM ${base}

ARG python_version=3.13
ARG python_patch_version=3.13.7

RUN apk add --no-cache \
    bash \
    build-base \
    bzip2-dev \
    g++ \
    git \
    libffi-dev \
    libnsl-dev \
    libtirpc-dev \
    linux-headers \
    ncurses-dev \
    openssl-dev \
    pkgconf \
    tzdata \
    zlib-dev

# Install Python without GIL
RUN wget https://github.com/python/cpython/archive/refs/tags/v${python_patch_version}.tar.gz && \
    tar -xzf v${python_patch_version}.tar.gz && \
    rm v${python_patch_version}.tar.gz && \
    cd cpython-${python_patch_version}/ && \
    ./configure --disable-gil --with-ensurepip && \
    make -j && \
    make install && \
    cd ../ && \
    rm -rf cpython-${python_patch_version}/

ENV ARROW_PYTHON_VENV /arrow-dev
RUN python${python_version}t -m venv ${ARROW_PYTHON_VENV}

ENV PYTHON_GIL 0
ENV PATH "${ARROW_PYTHON_VENV}/bin:${PATH}"
