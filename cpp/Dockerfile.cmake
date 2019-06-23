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

FROM ubuntu:18.04

# install build essentials
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        ca-certificates \
        curl \
        ccache \
        g++ \
        gcc \
        git \
        libidn11 \
        ninja-build \
        pkg-config \
        tzdata \
        wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# install conda and required packages
ARG EXTRA_CONDA_PKGS
ENV PATH=/opt/conda/bin:$PATH \
    CONDA_PREFIX=/opt/conda
COPY ci/docker_install_conda.sh \
     ci/conda_env_cpp.yml \
     ci/conda_env_unix.yml \
     /arrow/ci/
RUN arrow/ci/docker_install_conda.sh && \
    conda install -q -c conda-forge \
        --file arrow/ci/conda_env_cpp.yml \
        --file arrow/ci/conda_env_unix.yml \
        $EXTRA_CONDA_PKGS && \
    conda clean --all

ARG CMAKE_VERSION=3.2.3
ARG CMAKE_URL=https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-Linux-x86_64.tar.gz
RUN curl ${CMAKE_URL} -Lo cmake-${CMAKE_VERSION}.tar.gz && \
  mkdir /opt/cmake && tar -zxvf cmake-${CMAKE_VERSION}.tar.gz -C /opt/cmake
ENV PATH=/opt/cmake/cmake-${CMAKE_VERSION}-Linux-x86_64/bin:$PATH

ENV CC=gcc \
    CXX=g++ \
    ARROW_GANDIVA=OFF \
    ARROW_BUILD_TESTS=ON \
    ARROW_DEPENDENCY_SOURCE=CONDA \
    ARROW_HOME=$CONDA_PREFIX \
    PARQUET_HOME=$CONDA_PREFIX

# build and test
CMD ["arrow/ci/docker_build_and_test_cpp.sh"]
