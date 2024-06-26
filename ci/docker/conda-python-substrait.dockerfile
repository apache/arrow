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

# Note: openjdk is pinned to 17 because the
# substrait repo currently pins to jdk 17.
# Newer jdk versions are currently failing
# due to the recent upgrade to Gradle 8 via
# install_substrait_consumer.sh.
# https://github.com/substrait-io/substrait-java/issues/274
RUN mamba install -q -y \
        --file arrow/ci/conda_env_python.txt \
        --file arrow/ci/conda_env_sphinx.txt \
        $([ "$python" == "3.9" ] && echo "pickle5") \
        python=${python} \
        openjdk=17 \
        nomkl && \
    mamba clean --all


ARG substrait=latest
COPY ci/scripts/install_substrait_consumer.sh /arrow/ci/scripts/

RUN /arrow/ci/scripts/install_substrait_consumer.sh

ENV ARROW_ACERO=ON \
    ARROW_COMPUTE=ON \
    ARROW_CSV=ON \
    ARROW_DATASET=ON \
    ARROW_FILESYSTEM=ON \
    ARROW_FLIGHT=OFF \
    ARROW_FLIGHT_SQL=OFF \
    ARROW_GANDIVA=OFF \
    ARROW_JSON=ON \
    ARROW_SUBSTRAIT=ON \
    ARROW_TESTING=OFF
