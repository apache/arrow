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

ARG arch
ARG python_image_tag
FROM ${arch}/python:${python_image_tag}

# pandas doesn't provide wheel for aarch64 yet, so cache the compiled
# test dependencies in a docker image
COPY python/requirements-wheel-test.txt /arrow/python/
RUN pip install -r /arrow/python/requirements-wheel-test.txt

# Install the GCS testbench with the system Python
RUN apt-get update -y -q && \
    apt-get install -y -q \
        build-essential \
        python3-dev \
        tzdata-legacy && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

COPY ci/scripts/install_gcs_testbench.sh /arrow/ci/scripts/
ENV PIPX_PYTHON=/usr/bin/python3 PIPX_PIP_ARGS=--prefer-binary
RUN /arrow/ci/scripts/install_gcs_testbench.sh default
