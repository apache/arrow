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

RUN apt-get update && \
    apt-get install -y \
        gcc \
        g++ \
        git \
        wget \
        pkg-config \
        ninja-build

ENV CC=gcc \
    CXX=g++ \
    PATH=/opt/conda/bin:$PATH \
    CONDA_PREFIX=/opt/conda

# install dependencies
ARG PYTHON_VERSION=3.6
ADD ci/docker_install_conda.sh \
    ci/conda_env_cpp.yml \
    ci/conda_env_python.yml \
    /arrow/ci/
RUN arrow/ci/docker_install_conda.sh && \
    conda install -c conda-forge \
        --file arrow/ci/conda_env_cpp.yml \
        --file arrow/ci/conda_env_python.yml \
        python=$PYTHON_VERSION && \
    conda clean --all

# build cpp without tests
ENV ARROW_PYTHON=ON \
    ARROW_BUILD_TESTS=OFF
ADD ci/docker_build_cpp.sh /arrow/ci/
ADD cpp /arrow/cpp
ADD format /arrow/format
ADD java/pom.xml /arrow/java/pom.xml
RUN arrow/ci/docker_build_cpp.sh

# build python
ADD ci/docker_build_python.sh /arrow/ci/
ADD python /arrow/python
RUN arrow/ci/docker_build_python.sh

WORKDIR arrow/python
CMD pytest -v pyarrow
