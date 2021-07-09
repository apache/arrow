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

set -e

declare -A archs
archs=([amd64]=x86_64
       [arm32v7]=armv7l
       [ppc64le]=ppc64le
       [i386]=x86)

declare -A platforms
platforms=([windows]=Windows
           [macos]=MacOSX
           [linux]=Linux)

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <architecture> <platform> <version>"
  exit 1
elif [[ -z ${archs[$1]} ]]; then
  echo "Unexpected architecture: ${1}"
  exit 1
elif [[ -z ${platforms[$2]} ]]; then
  echo "Unexpected platform: ${2}"
  exit 1
fi

arch=${archs[$1]}
platform=${platforms[$2]}
version=$3
short_version=${version:0:3}

if [ $platform == "MacOSX" ]; then
    echo "Downloading Python installer..."
    if [ "$(uname -m)" = "arm64" ]; then
        fname="python-${version}-macos11.pkg"
    else
        fname="python-${version}-macosx10.9.pkg"
    fi
    wget "https://www.python.org/ftp/python/${version}/${fname}"

    echo "Installing Python..."
    installer -pkg $fname -target /
    rm $fname

    echo "Installing Pip..."
    python="/Library/Frameworks/Python.framework/Versions/${short_version}/bin/python${short_version}"
    pip="${python} -m pip"

    $python -m ensurepip
    $pip install -U pip setuptools virtualenv

    # Install certificates for Python 3.6
    # local inst_cmd="/Applications/Python ${py_mm}/Install Certificates.command"
    # if [ -e "$inst_cmd" ]; then
    #     sh "$inst_cmd"
    # fi
else
    echo "Unsupported platform: $platform"
fi
