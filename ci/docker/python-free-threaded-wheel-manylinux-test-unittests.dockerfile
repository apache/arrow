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

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y -q && \
    apt install -y -q --no-install-recommends software-properties-common gpg-agent && \
    add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get update -y -q && \
    apt install -y -q --no-install-recommends \
        build-essential \
        libffi-dev \
        python3.13-dev \
        python3.13-nogil \
        python3.13-venv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

ENV ARROW_PYTHON_VENV /arrow-dev
RUN python3.13t -m venv ${ARROW_PYTHON_VENV}

ENV PYTHON_GIL 0
ENV PATH "${ARROW_PYTHON_VENV}/bin:${PATH}"

# pandas doesn't provide wheels for aarch64 yet, so we have to install nightly Cython
# along with the rest of pandas' build dependencies and disable build isolation
COPY python/requirements-wheel-test.txt /arrow/python/
RUN python -m pip install \
    --pre \
    --prefer-binary \
    --extra-index-url "https://pypi.anaconda.org/scientific-python-nightly-wheels/simple" \
    Cython numpy
RUN python -m pip install "meson-python==0.13.1" "meson==1.2.1" wheel "versioneer[toml]" ninja
RUN python -m pip install --no-build-isolation -r /arrow/python/requirements-wheel-test.txt
