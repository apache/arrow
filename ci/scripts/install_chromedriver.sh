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

# Look up the Chrome version from the apt repo's Packages file.
CHROME_DEB_VERSION=$(wget --no-verbose -O - \
  "https://dl.google.com/linux/chrome/deb/dists/stable/main/binary-amd64/Packages.gz" \
  | gunzip \
  | awk '/^Package: google-chrome-stable$/{found=1} found && /^Version: /{print $2; exit}')
CHROME_VERSION_FULL=${CHROME_DEB_VERSION%-*}

# Validate there hasn't been major version bumps since the last time we updated this script.
if [ "$chrome_version" != "latest" ] && [ "${CHROME_VERSION_FULL%%.*}" != "$chrome_version" ]; then
  echo "Requested Chrome major ${chrome_version}, but apt repo currently publishes ${CHROME_VERSION_FULL}" >&2
  exit 1
fi

CHROME_DOWNLOAD_URL="https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_${CHROME_DEB_VERSION}_amd64.deb"
CHROMEDRIVER_DOWNLOAD_URL="https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION_FULL}/linux64/chromedriver-linux64.zip"
wget --no-verbose -O /tmp/google-chrome.deb "${CHROME_DOWNLOAD_URL}"
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
