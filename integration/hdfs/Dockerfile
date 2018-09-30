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

FROM gelog/hadoop

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

# installing in the previous step boost=1.60 and boost-cpp=1.67 gets installed,
# cmake finds 1.60 and parquet fails to compile
# installing it in a separate step, boost=1.60 and boost-cpp=1.64 gets
# installed, cmake finds 1.64
# libhdfs3 needs to be pinned, see ARROW-1465 and ARROW-1445
RUN conda install -y -c conda-forge hdfs3 libhdfs3=2.2.31 && \
    conda clean --all

# build cpp with tests
ENV ARROW_HDFS=ON \
    ARROW_PYTHON=ON \
    ARROW_BUILD_TESTS=ON \
    LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${HADOOP_HOME}/lib/native"
ADD ci/docker_build_cpp.sh /arrow/ci/
ADD cpp /arrow/cpp
ADD format /arrow/format
ADD java/pom.xml /arrow/java/pom.xml
RUN arrow/ci/docker_build_cpp.sh

# build python
ADD ci/docker_build_python.sh /arrow/ci/
ADD python /arrow/python
RUN arrow/ci/docker_build_python.sh

# execute integration tests
ENV LIBHDFS3_CONF=/arrow/integration/hdfs/libhdfs3.xml
ADD integration /arrow/integration
CMD arrow/integration/hdfs/runtest.sh
