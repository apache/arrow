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

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get -q update && \
    apt-get -q install --no-install-recommends -y \
        gcc \
        g++ \
        git \
        wget \
        tzdata \
        ruby-dev \
        pkg-config \
        ninja-build \
        autoconf-archive \
        gtk-doc-tools \
        libgirepository1.0-dev

ENV CC=gcc \
    CXX=g++ \
    PATH=/opt/conda/bin:$PATH \
    CONDA_PREFIX=/opt/conda

# install dependencies
ADD ci/docker_install_conda.sh \
    ci/conda_env_cpp.yml \
    /arrow/ci/
ADD c_glib/Gemfile /arrow/c_glib/
RUN arrow/ci/docker_install_conda.sh && \
    conda install -c conda-forge \
        --file arrow/ci/conda_env_cpp.yml \
        meson=0.47.1 && \
    conda clean --all && \
    gem install bundler && \
    bundle install --gemfile arrow/c_glib/Gemfile

# build cpp
ENV ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_INSTALL_NAME_RPATH=OFF
ADD ci/docker_build_cpp.sh /arrow/ci/
ADD cpp /arrow/cpp
ADD format /arrow/format
ADD java/pom.xml /arrow/java/pom.xml
RUN arrow/ci/docker_build_cpp.sh

# build c_glib
ENV LD_LIBRARY_PATH="${CONDA_PREFIX}/lib" \
    PKG_CONFIG_PATH="${CONDA_PREFIX}/lib/pkgconfig" \
    GI_TYPELIB_PATH="${CONDA_PREFIX}/lib/girepository-1.0"
ADD ci/docker_build_c_glib.sh /arrow/ci/
ADD c_glib /arrow/c_glib
RUN arrow/ci/docker_build_c_glib.sh

WORKDIR arrow/c_glib
CMD test/run-test.rb
