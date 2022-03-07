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

# install python specific packages
ARG python=3.8
COPY ci/conda_env_python.txt \
     ci/conda_env_sphinx.txt \
     /arrow/ci/
RUN mamba install -q \
        --file arrow/ci/conda_env_python.txt \
        --file arrow/ci/conda_env_sphinx.txt \
        $([ "$python" == "3.7" ] && echo "pickle5") \
        python=${python} \
        nomkl && \
    mamba clean --all

# unable to install from conda-forge due to sphinx version pin, see comment in
# arrow/ci/conda_env_sphinx.txt
RUN pip install sphinx-tabs

ENV ARROW_PYTHON=ON \
    ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_TENSORFLOW=ON \
    ARROW_USE_GLOG=OFF \
    ARROW_HDFS=ON
