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

set -eu

declare -A platforms
platforms=([windows]=Windows
           [macos]=MacOSX
           [linux]=Linux)

declare -A versions
versions=([3.7]=3.7.9
          [3.8]=3.8.10
          [3.9]=3.9.13
          [3.10]=3.10.8
          [3.11]=3.11.0)

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <platform> <version>"
  exit 1
elif [[ -z ${platforms[$1]} ]]; then
  echo "Unexpected platform: ${1}"
  exit 1
fi

platform=${platforms[$1]}
version=$2
full_version=${versions[$2]}

if [ $platform = "MacOSX" ]; then
    echo "Downloading Python installer..."

    if [ "$(uname -m)" = "arm64" ] || [ "$version" = "3.10" ] || [ "$version" = "3.11" ]; then
        fname="python-${full_version}-macos11.pkg"
    else
        fname="python-${full_version}-macosx10.9.pkg"
    fi
    wget "https://www.python.org/ftp/python/${full_version}/${fname}"

    echo "Installing Python..."
    installer -pkg $fname -target /
    rm $fname

    echo "Installing Pip..."
    python="/Library/Frameworks/Python.framework/Versions/${version}/bin/python${version}"
    pip="${python} -m pip"

    $python -m ensurepip
    $pip install -U pip setuptools
else
    echo "Unsupported platform: $platform"
fi
