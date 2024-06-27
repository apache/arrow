#!/usr/bin/env bash
#
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

# Install Chrome and Chromedriver for Selenium

set -e

chrome_version=$1

if [ $chrome_version = "latest" ]; then
  latest_release_path=LATEST_RELEASE_STABLE
else
  latest_release_path=LATEST_RELEASE_${chrome_version}
fi
CHROME_VERSION_FULL=$(wget -q --no-verbose -O - "https://googlechromelabs.github.io/chrome-for-testing/${latest_release_path}")
CHROME_DOWNLOAD_URL="https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_${CHROME_VERSION_FULL}-1_amd64.deb"
CHROMEDRIVER_DOWNLOAD_URL="https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION_FULL}/linux64/chromedriver-linux64.zip"
wget -q --no-verbose -O /tmp/google-chrome.deb "${CHROME_DOWNLOAD_URL}"
apt-get update
apt install -qqy /tmp/google-chrome.deb
rm -f /tmp/google-chrome.deb
rm -rf /var/lib/apt/lists/*
wget --no-verbose -O /tmp/chromedriver-linux64.zip "${CHROMEDRIVER_DOWNLOAD_URL}"
unzip /tmp/chromedriver-linux64.zip -d /opt/
rm /tmp/chromedriver-linux64.zip
ln -fs /opt/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver
echo "Using Chrome version: $(google-chrome --version)"
echo "Using Chrome Driver version: $(chromedriver --version)"
