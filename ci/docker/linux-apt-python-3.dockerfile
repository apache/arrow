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

COPY python/requirements-build.txt \
     python/requirements-test.txt \
     /arrow/python/

ENV ARROW_PYTHON_VENV /arrow-dev

ARG ubuntu
ARG python="3.12"
COPY ci/scripts/install_python.sh /arrow/ci/scripts/
RUN if [ "${ubuntu}" = "20.04" ]; then \
        quiet=$([ "${DEBUG}" = "yes" ] || echo "-qq") && \
        apt update ${quiet} && \
        apt install -y -V ${quiet} \
          xz-utils && \
        apt clean && \
        rm -rf /var/lib/apt/lists/* && \
        /arrow/ci/scripts/install_python.sh linux ${python} \
    ; fi

RUN python3 -m venv ${ARROW_PYTHON_VENV} && \
    . ${ARROW_PYTHON_VENV}/bin/activate && \
    pip install -U pip setuptools wheel && \
    pip install \
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
    ARROW_GDB=ON \
    ARROW_HDFS=ON \
    ARROW_JSON=ON \
    ARROW_USE_GLOG=OFF
