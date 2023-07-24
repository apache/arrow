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
ARG python=3.9

FROM ${repo}:${arch}-conda-python-${python}

COPY ci/conda_env_python.txt \
     ci/conda_env_sphinx.txt \
     /arrow/ci/
RUN mamba install -q -y \
        --file arrow/ci/conda_env_python.txt \
        --file arrow/ci/conda_env_sphinx.txt \
        $([ "$python" == "3.9" ] && echo "pickle5") \
        python=${python} openjdk \
        nomkl && \
    mamba clean --all


ARG substrait=latest
COPY ci/scripts/install_substrait_consumer.sh /arrow/ci/scripts/

ENV ARROW_ACERO=ON \
    ARROW_BUILD_TESTS=ON \
    ARROW_COMPUTE=ON \
    ARROW_CSV=ON \
    ARROW_DATASET=ON \
    ARROW_FILESYSTEM=ON \
    ARROW_JSON=ON \
    ARROW_SUBSTRAIT=ON

RUN /arrow/ci/scripts/install_substrait_consumer.sh
