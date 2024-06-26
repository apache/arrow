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
ARG emscripten_version="3.1.58"
ARG chrome_version="latest"
ARG required_python_min="(3,12)"
# fail if python version < 3.12
RUN echo "check PYTHON>=${required_python_min}" && python -c "import sys;sys.exit(0 if sys.version_info>=${required_python_min} else 1)"

# install selenium and pyodide-build and recent python

# needs to be a login shell so ~/.profile is read
SHELL ["/bin/bash","--login","-c","-o","pipefail"]

RUN python --version && \
  python -m pip install --no-cache-dir selenium==${selenium_version} &&\
  python -m pip install --no-cache-dir --upgrade --pre pyodide-build==${pyodide_version}

# hadolint ignore=DL3003
RUN cd ~ && (ls emsdk || git clone https://github.com/emscripten-core/emsdk.git) && \
    cd emsdk && \
    ./emsdk install ${emscripten_version} && \
    ./emsdk activate ${emscripten_version} && \
    echo "Installed emsdk to:" ~/emsdk
    
# make sure zlib is cached in the EMSDK folder
RUN source ~/emsdk/emsdk_env.sh && embuilder --pic build zlib

# install pyodide dist directory to /pyodide
WORKDIR /
RUN pyodide_dist_url="https://github.com/pyodide/pyodide/releases/download/${pyodide_version}/pyodide-${pyodide_version}.tar.bz2"\
  && wget --progress=dot:giga "${pyodide_dist_url}" -O- |tar -xj

# install chrome for testing browser based runner
# zip needed for chrome, libpthread stubs installs pthread module to cmake, build-essential makes
# sure that unix make is available in isolated python environments

# Install basic build stuff, don't pin versions, hence ignore lint
# hadolint ignore=DL3008
RUN apt-get update && apt-get install --no-install-recommends -y -q unzip zip libpthread-stubs0-dev build-essential\
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# We use apt to install deb file + deps here
# so ignore lint comment
# hadolint ignore=DL3027
RUN if [ $chrome_version = "latest" ]; \
  then CHROME_VERSION_FULL=$(wget --progress=dot:giga --no-verbose -O - "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_STABLE"); \
  else CHROME_VERSION_FULL=$(wget --progress=dot:giga --no-verbose -O - "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_${chrome_version}"); \
  fi \
  && CHROME_DOWNLOAD_URL="https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_${CHROME_VERSION_FULL}-1_amd64.deb" \
  && CHROMEDRIVER_DOWNLOAD_URL="https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION_FULL}/linux64/chromedriver-linux64.zip" \
  && wget --progress=dot:giga --no-verbose -O /tmp/google-chrome.deb "${CHROME_DOWNLOAD_URL}" \
  && apt-get update \
  && apt install -qqy /tmp/google-chrome.deb \
  && rm -f /tmp/google-chrome.deb \
  && rm -rf /var/lib/apt/lists/* \
  && wget --no-verbose -O /tmp/chromedriver-linux64.zip "${CHROMEDRIVER_DOWNLOAD_URL}" \
  && unzip /tmp/chromedriver-linux64.zip -d /opt/ \
  && rm /tmp/chromedriver-linux64.zip \
  && ln -fs /opt/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
  && echo "Using Chrome version: $(google-chrome --version)" \
  && echo "Using Chrome Driver version: $(chromedriver --version)"
  


SHELL ["/bin/bash", "--login", "-i", "-o","pipefail","-c"]
# install node 18 (needed for async call support)
RUN wget -q https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh -O- | bash -
RUN nvm install 18
SHELL ["/bin/bash","--login","-c"]
