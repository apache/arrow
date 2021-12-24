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
       [arm64v8]=aarch64)

declare -A platforms
platforms=([linux]=linux
           [macos]=macos
           [windows]=windows)

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <architecture> <platform> <version> <prefix>"
  exit 1
fi

arch=${archs[$1]}
platform=${platforms[$2]}
version=$3
prefix=$4

url="https://github.com/Kitware/CMake/releases/download/v${version}/cmake-${version}-${platform}-${arch}.tar.gz"
wget -q ${url} -O - | tar -xzf - --directory ${prefix} --strip-components=1
