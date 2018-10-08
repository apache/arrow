#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM ubuntu:18.04

# Basic OS dependencies
# First install JDK 8, then do a second apt call to install maven.
# Otherwise this will install openjdk 8 and openjdk 11 at once.
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk-headless && \
    apt-get install -y \
          gcc-8 \
          g++-8 \
          git \
          wget \
          make \
          ninja-build \
          rsync \
          maven \
          pkg-config \
          gobject-introspection \
          libgirepository1.0-dev \
          gtk-doc-tools \
          libtool \
          autoconf-archive \
          npm

ENV CC=gcc-8 \
    CXX=g++-8 \
    PATH=/opt/conda/bin:$PATH \
    CONDA_PREFIX=/opt/conda

ADD ci/docker_install_conda.sh \
    ci/conda_env_cpp.yml \
    ci/conda_env_python.yml \
    /arrow/ci/
RUN arrow/ci/docker_install_conda.sh && \
    conda install -c conda-forge \
        --file arrow/ci/conda_env_cpp.yml \
        --file arrow/ci/conda_env_python.yml \
        numpydoc \
        sphinx \
        sphinx_bootstrap_theme \
        doxygen \
        maven && \
    conda clean --all -y

ADD . /arrow

CMD arrow/dev/gen_apidocs/create_documents.sh
