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

ARG emscripten_version="3.1.46"
RUN cd ~ && (ls emsdk || git clone https://github.com/emscripten-core/emsdk.git) && \
    cd emsdk && \
    ./emsdk install ${emscripten_version} && \
    ./emsdk activate ${emscripten_version} && \
    echo "Installed emsdk to:" ~/emsdk

# install pyodide dist directory to /pyodide
ARG pyodide_version="0.25.1"
RUN cd / \
  && pyodide_dist_url="https://github.com/pyodide/pyodide/releases/download/${pyodide_version}/pyodide-0.25.1.tar.bz2"\
  && wget ${pyodide_dist_url} -O- |tar -xj

# install browsers

ARG chrome_version="latest"
ARG firefox_version="latest"
# Note: geckodriver version needs to be updated manually
ARG geckodriver_version="0.34.0"

#============================================
# Firefox & geckodriver
#============================================
# can specify Firefox version by firefox_version;
#  e.g. latest
#       95
#       96
#
# can specify Firefox geckodriver version by geckodriver_version;
#============================================

RUN if [ $firefox_version = "latest" ] || [ $firefox_version = "nightly-latest" ] || [ $firefox_version = "devedition-latest" ] || [ $firefox_version = "esr-latest" ]; \
  then FIREFOX_DOWNLOAD_URL="https://download.mozilla.org/?product=firefox-$firefox_version-ssl&os=linux64&lang=en-US"; \
  else FIREFOX_VERSION_FULL="${firefox_version}.0" && FIREFOX_DOWNLOAD_URL="https://download-installer.cdn.mozilla.net/pub/firefox/releases/$FIREFOX_VERSION_FULL/linux-x86_64/en-US/firefox-$FIREFOX_VERSION_FULL.tar.bz2"; \
  fi \
  && wget --no-verbose -O /tmp/firefox.tar.bz2 $FIREFOX_DOWNLOAD_URL \
  && tar -C /opt -xjf /tmp/firefox.tar.bz2 \
  && rm /tmp/firefox.tar.bz2 \
  && mv /opt/firefox /opt/firefox-$firefox_version \
  && ln -fs /opt/firefox-$firefox_version/firefox /usr/local/bin/firefox \
  && wget --no-verbose -O /tmp/geckodriver.tar.gz https://github.com/mozilla/geckodriver/releases/download/v$geckodriver_version/geckodriver-v$geckodriver_version-linux64.tar.gz \
  && rm -rf /opt/geckodriver \
  && tar -C /opt -zxf /tmp/geckodriver.tar.gz \
  && rm /tmp/geckodriver.tar.gz \
  && mv /opt/geckodriver /opt/geckodriver-$geckodriver_version \
  && chmod 755 /opt/geckodriver-$geckodriver_version \
  && ln -fs /opt/geckodriver-$geckodriver_version /usr/local/bin/geckodriver \
  && echo "Using Firefox version: $(firefox --version)" \
  && echo "Using GeckoDriver version: "$geckodriver_version

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
  
ARG selenium_version="4.15.2"

SHELL ["/bin/bash", "--login", "-i", "-c"]
# install node 18 (needed for async call support)
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash -
RUN nvm install 18
SHELL ["/bin/bash","-c"]

# install selenium and pyodide-build and recent python
 RUN    apt-get update && apt install "software-properties-common" -y -q &&\
     add-apt-repository ppa:deadsnakes/ppa &&\
     apt install python3.11 -y -q &&\
     ln -s /usr/bin/python3.11 /usr/bin/python && \
     python -m pip install selenium==${selenium_version} &&\
     python -m pip install pyodide-build==${pyodide_version}
