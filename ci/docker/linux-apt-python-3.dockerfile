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

RUN apt-get update -y -q && \
    apt-get install -y -q \
        python3 \
        python3-pip \
        python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/local/bin/python && \
    ln -s /usr/bin/pip3 /usr/local/bin/pip

RUN pip install -U pip setuptools wheel

COPY python/requirements-build.txt \
     python/requirements-test.txt \
     /arrow/python/

RUN pip install \
    -r arrow/python/requirements-build.txt \
    -r arrow/python/requirements-test.txt

ARG numba
COPY ci/scripts/install_numba.sh /arrow/ci/scripts/
RUN if [ "${numba}" != "" ]; then \
        /arrow/ci/scripts/install_numba.sh ${numba} \
    ; fi

ENV ARROW_ACERO=ON \
    ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_COMPUTE=ON \
    ARROW_CSV=ON \
    ARROW_DATASET=ON \
    ARROW_FILESYSTEM=ON \
    ARROW_HDFS=ON \
    ARROW_JSON=ON \
    ARROW_USE_GLOG=OFF
