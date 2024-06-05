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
FROM ${repo}:${arch}-ubuntu-22.04-cpp

ARG selenium_version="4.15.2"
ARG pyodide_version="0.26.0"
ARG emscripten_version="3.1.58"
ARG chrome_version="latest"

# install selenium and pyodide-build and recent python

# needs to be a login shell so ~/.profile is read
SHELL ["/bin/bash","--login","-c"]

RUN wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh -O ~/miniforge.sh

RUN bash ~/miniforge.sh -b -p /miniforge &&\
    source "/miniforge/etc/profile.d/conda.sh" &&\
    source "/miniforge/etc/profile.d/mamba.sh"  &&\
    echo ". /miniforge/etc/profile.d/conda.sh" >> ~/.profile &&\
    echo ". /miniforge/etc/profile.d/mamba.sh" >> ~/.profile &&\
    conda init bash
    

#ENV PATH /miniforge/bin:$PATH

RUN  conda create -y -n py312 python=3.12 -c conda-forge &&\
     echo "conda activate py312" >> ~/.profile &&\
     echo "conda activate py312" >> ~/.bashrc


#RUN conda init bash && cat ~/.profile && false

RUN python --version && \
python -m pip install selenium==${selenium_version} &&\
    python -m pip install --upgrade --pre pyodide-build==${pyodide_version}

RUN cd ~ && (ls emsdk || git clone https://github.com/emscripten-core/emsdk.git) && \
    cd emsdk && \
    ./emsdk install ${emscripten_version} && \
    ./emsdk activate ${emscripten_version} && \
    echo "Installed emsdk to:" ~/emsdk

# install pyodide dist directory to /pyodide
RUN cd / \
  && pyodide_dist_url="https://github.com/pyodide/pyodide/releases/download/${pyodide_version}/pyodide-${pyodide_version}.tar.bz2"\
  && wget ${pyodide_dist_url} -O- |tar -xj

# install chrome for testing browser based runner

RUN apt install -y -q zip

RUN if [ $chrome_version = "latest" ]; \
  then CHROME_VERSION_FULL=$(wget --no-verbose -O - "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_STABLE"); \
  else CHROME_VERSION_FULL=$(wget --no-verbose -O - "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_${chrome_version}"); \
  fi \
  && CHROME_DOWNLOAD_URL="https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_${CHROME_VERSION_FULL}-1_amd64.deb" \
  && CHROMEDRIVER_DOWNLOAD_URL="https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION_FULL}/linux64/chromedriver-linux64.zip" \
  && wget --no-verbose -O /tmp/google-chrome.deb ${CHROME_DOWNLOAD_URL} \
  && apt-get update \
  && apt install -qqy /tmp/google-chrome.deb \
  && rm -f /tmp/google-chrome.deb \
  && rm -rf /var/lib/apt/lists/* \
  && wget --no-verbose -O /tmp/chromedriver-linux64.zip ${CHROMEDRIVER_DOWNLOAD_URL} \
  && unzip /tmp/chromedriver-linux64.zip -d /opt/ \
  && rm /tmp/chromedriver-linux64.zip \
  && ln -fs /opt/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
  && echo "Using Chrome version: $(google-chrome --version)" \
  && echo "Using Chrome Driver version: $(chromedriver --version)"
  


SHELL ["/bin/bash", "--login", "-i", "-c"]
# install node 18 (needed for async call support)
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash -
RUN nvm install 18
SHELL ["/bin/bash","--login","-c"]

