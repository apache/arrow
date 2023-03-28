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

ARG arch=amd64
FROM ${arch}/ubuntu:22.04

# install build essentials
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update -y -q && \
    apt-get install -y -q curl wget tzdata libc6-dbg gdb \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# install conda and mamba via mambaforge
COPY ci/scripts/install_conda.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_conda.sh mambaforge latest /opt/conda
ENV PATH=/opt/conda/bin:$PATH

# create a conda environment
ADD ci/conda_env_unix.txt /arrow/ci/
RUN mamba create -n arrow --file arrow/ci/conda_env_unix.txt git && \
    mamba clean --all

# activate the created environment by default
RUN echo "conda activate arrow" >> ~/.profile
ENV CONDA_PREFIX=/opt/conda/envs/arrow

# use login shell to activate arrow environment un the RUN commands
SHELL ["/bin/bash", "-c", "-l"]

# use login shell when running the container
ENTRYPOINT ["/bin/bash", "-c", "-l"]
