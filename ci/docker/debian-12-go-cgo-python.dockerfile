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

ENV DEBIAN_FRONTEND noninteractive

# Install python3 and pip so we can install pyarrow to test the C data interface.
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv && \
    apt-get clean

ENV ARROW_PYTHON_VENV /arrow-dev
RUN python3 -m venv ${ARROW_PYTHON_VENV} && \
    . ${ARROW_PYTHON_VENV}/bin/activate && \
    pip install pyarrow cffi --only-binary pyarrow
