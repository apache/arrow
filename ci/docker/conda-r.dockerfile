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

ARG repo
ARG arch
FROM ${repo}:${arch}-conda-cpp

# Need locales so we can set UTF-8
RUN apt-get update -y && \
    apt-get install -y locales && \
    locale-gen en_US.UTF-8 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# install R specific packages
ARG r=3.6.1
COPY ci/conda_env_r.yml /arrow/ci/
RUN conda install -q \
        --file arrow/ci/conda_env_r.yml \
        r-base=$r \
        nomkl && \
    conda clean --all

# Ensure parallel compilation of of C/C++ code
RUN echo "MAKEFLAGS=-j$(R -s -e 'cat(parallel::detectCores())')" >> $CONDA_PREFIX/lib/R/etc/Makeconf

ENV ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_DEPENDENCY_SOURCE=SYSTEM \
    ARROW_FLIGHT=OFF \
    ARROW_GANDIVA=OFF \
    ARROW_NO_DEPRECATED_API=ON \
    ARROW_ORC=OFF \
    ARROW_PARQUET=ON \
    ARROW_PLASMA=OFF \
    ARROW_USE_CCACHE=ON \
    ARROW_USE_GLOG=OFF \
    LC_ALL=en_US.UTF-8
