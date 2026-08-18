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

ARG arch=amd64
ARG base=ubuntu:22.04
FROM --platform=linux/${arch} ${base}

ENV DEBIAN_FRONTEND=noninteractive
COPY dev/release/setup-ubuntu.sh /
RUN INSTALL_PYTHON=0 /setup-ubuntu.sh && \
    rm /setup-ubuntu.sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

# Ubuntu 22.04 only provides Python 3.10. Install a supported Python from
# conda-forge (without activating Conda env) for non-Conda verify-rc job
ARG python=3.12
COPY ci/scripts/install_conda.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_conda.sh miniforge3 26.1.1-3 /opt/conda && \
    /opt/conda/bin/mamba create -y -p /opt/python python=${python} && \
    rm -rf \
        /etc/profile.d/conda.sh \
        /opt/conda \
        /root/.conda \
        /root/.condarc \
        /root/.mamba
ENV PATH=/opt/python/bin:$PATH \
    PYTHON_VERSION=${python}

ARG cmake
COPY ci/scripts/install_cmake.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_cmake.sh ${cmake} /usr/local/
