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
ARG arch=amd64
ARG python=3.8
FROM ${repo}:${arch}-conda-python-${python}

ARG jdk=8
ARG maven=3.5

ARG numpy=latest
COPY ci/scripts/install_numpy.sh /arrow/ci/scripts/

RUN mamba install -q -y \
        openjdk=${jdk} \
        maven=${maven} \
        pandas && \
    mamba clean --all && \
    mamba uninstall -q -y numpy && \
    /arrow/ci/scripts/install_numpy.sh ${numpy}

# installing specific version of spark
ARG spark=master
COPY ci/scripts/install_spark.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_spark.sh ${spark} /spark

# build cpp with tests
ENV CC=gcc \
    CXX=g++ \
    ARROW_ACERO=ON \
    ARROW_BUILD_TESTS=OFF \
    ARROW_COMPUTE=ON \
    ARROW_CSV=ON \
    ARROW_DATASET=ON \
    ARROW_FILESYSTEM=ON \
    ARROW_HDFS=ON \
    ARROW_JSON=ON \
    SPARK_VERSION=${spark}
