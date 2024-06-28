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
ARG python="3.12"
FROM ${repo}:${arch}-conda-python-${python}

ARG selenium_version="4.15.2"
ARG pyodide_version="0.26.0"
ARG chrome_version="latest"
ARG required_python_min="(3,12)"
# fail if python version < 3.12
RUN echo "check PYTHON>=${required_python_min}" && python -c "import sys;sys.exit(0 if sys.version_info>=${required_python_min} else 1)"

# install selenium and pyodide-build and recent python

# needs to be a login shell so ~/.profile is read
SHELL ["/bin/bash", "--login", "-c", "-o", "pipefail"]

RUN python -m pip install --no-cache-dir selenium==${selenium_version} && \
    python -m pip install --no-cache-dir --upgrade --pre pyodide-build==${pyodide_version}
    
# install pyodide dist directory to /pyodide
RUN pyodide_dist_url="https://github.com/pyodide/pyodide/releases/download/${pyodide_version}/pyodide-${pyodide_version}.tar.bz2" && \
    wget -q "${pyodide_dist_url}" -O- | tar -xj -C /

# install correct version of emscripten for this pyodide
COPY ci/scripts/install_emscripten.sh /arrow/ci/scripts/
RUN bash /arrow/ci/scripts/install_emscripten.sh ~ /pyodide

# make sure zlib is cached in the EMSDK folder
RUN source ~/emsdk/emsdk_env.sh && embuilder --pic build zlib

# install chrome for testing browser based runner
# zip needed for chrome, libpthread stubs installs pthread module to cmake, build-essential makes
# sure that unix make is available in isolated python environments

# Install basic build stuff, don't pin versions, hence ignore lint
# hadolint ignore=DL3008
RUN apt-get update && apt-get install --no-install-recommends -y -q unzip zip libpthread-stubs0-dev build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ci/scripts/install_chromedriver.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/install_chromedriver.sh "${chrome_version}"

SHELL ["/bin/bash", "--login", "-i", "-o", "pipefail", "-c"]
# install node 18 (needed for async call support)
RUN conda install nodejs=18 -c conda-forge
SHELL ["/bin/bash", "--login", "-c"]

ENV ARROW_BUILD_TESTS="OFF" \
    ARROW_BUILD_TYPE="release" \
    ARROW_DEPENDENCY_SOURCE="BUNDLED" \
    ARROW_EMSCRIPTEN="ON" \
    PYODIDE_VERSION=${pyodide_version}
