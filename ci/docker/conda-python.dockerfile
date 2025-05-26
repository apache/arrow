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
ARG python=3.9
COPY ci/conda_env_python.txt \
     /arrow/ci/
# If the Python version being used is 3.10 we need to install gdb<16.3 (GH-46343).
# If the Python version being tested is the same as the Python used by the system gdb,
# we need to install the conda-forge gdb instead (GH-38323).
RUN if [ "$python" == "3.10" ]; then \
        GDB_PACKAGE="gdb<16.3"; \
    elif [ "$python" == "$(gdb --batch --eval-command 'python import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" ]; then \
        GDB_PACKAGE="gdb"; \
    else \
        GDB_PACKAGE=""; \
    fi && \
    mamba install -q -y \
        --file arrow/ci/conda_env_python.txt \
        $([[ -n "$GDB_PACKAGE" ]] && echo "$GDB_PACKAGE") \
        "python=${python}.*=*_cp*" \
        nomkl && \
    mamba clean --all --yes

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
    ARROW_SUBSTRAIT=OFF \
    ARROW_TENSORFLOW=ON \
    ARROW_USE_GLOG=OFF
