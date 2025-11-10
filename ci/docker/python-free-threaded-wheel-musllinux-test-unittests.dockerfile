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
ARG python_patch_version=3.13.9
ARG build_date=20251014
ARG arch=aarch64

RUN apk update && \
    apk add --no-cache \
    bash \
    build-base \
    curl \
    g++ \
    git \
    tar \
    tzdata \
    zstd

# Install Python with free-threading from python-build-standalone
# See available releases at: https://github.com/astral-sh/python-build-standalone/releases
RUN set -e; \
    case "${python_version}" in \
      3.13) python_patch_version="3.13.9";; \
      3.14) python_patch_version="3.14.0";; \
    esac && \
    curl -L -o python.tar.zst \
    https://github.com/astral-sh/python-build-standalone/releases/download/${build_date}/cpython-${python_patch_version}+${build_date}-${arch}-unknown-linux-musl-freethreaded+lto-full.tar.zst && \
    mkdir -p /opt/python && \
    tar -xf python.tar.zst -C /opt/python --strip-components=1 && \
    rm python.tar.zst

ENV PATH="/opt/python/install/bin:${PATH}"

ENV ARROW_PYTHON_VENV /arrow-dev
RUN python${python_version}t -m venv ${ARROW_PYTHON_VENV}

ENV PYTHON_GIL 0
ENV PATH "${ARROW_PYTHON_VENV}/bin:${PATH}"

ENV TZDIR=/usr/share/zoneinfo
RUN cp /usr/share/zoneinfo/Etc/UTC /etc/localtime

# pandas doesn't provide wheels for aarch64 yet, so we have to install nightly Cython
# along with the rest of pandas' build dependencies and disable build isolation
RUN python -m pip install \
    --pre \
    --prefer-binary \
    --extra-index-url "https://pypi.anaconda.org/scientific-python-nightly-wheels/simple" \
    Cython numpy
RUN python -m pip install "meson-python==0.13.1" "meson==1.2.1" wheel "versioneer[toml]" ninja
