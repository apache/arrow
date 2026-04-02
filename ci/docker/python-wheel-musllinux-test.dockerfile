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

ARG alpine_linux
ARG python_image_tag
FROM python:${python_image_tag}-alpine${alpine_linux}

# Re-define python_image_tag argument to be used in the next stage.
# This is needed because the argument is not preserved after the FROM statement.
ARG python_image_tag

RUN apk add --no-cache \
    bash \
    g++ \
    linux-headers \
    python3-dev \
    tzdata

ENV TZDIR=/usr/share/zoneinfo
RUN cp /usr/share/zoneinfo/Etc/UTC /etc/localtime

# pandas doesn't provide wheel for aarch64 yet, so cache the compiled
# test dependencies in a docker image
COPY python/requirements-wheel-test.txt /arrow/python/
# Pandas 2.0.3 is the last version to support numpy 1.21.x which is
# the lowest version we support for Python 3.10.
# Pandas 2.0.3 doesn't have wheels for Python 3.10 so we need to build from source,
# which requires setuptools < 80.
RUN if [ "${python_image_tag}" = "3.10" ]; then \
        echo 'setuptools<80' > /tmp/setuptools-constraint.txt; \
        PIP_CONSTRAINT=/tmp/setuptools-constraint.txt \
            pip install -r /arrow/python/requirements-wheel-test.txt; \
    else \
        pip install -r /arrow/python/requirements-wheel-test.txt; \
    fi

# Install the GCS testbench with the system Python
COPY ci/scripts/install_gcs_testbench.sh /arrow/ci/scripts/
ENV PIPX_PYTHON=/usr/bin/python3 PIPX_PIP_ARGS=--prefer-binary
RUN /arrow/ci/scripts/install_gcs_testbench.sh default
