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

set -ex

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <prefix>"
  exit 1
fi

declare -A archs
archs=([x86_64]=x86_64
       [arm64]=aarch64
       [aarch64]=aarch64)

arch=$(uname -m)
if [ -z ${archs[$arch]} ]; then
  echo "Unsupported architecture: ${arch}"
  exit 0
fi
arch=${archs[$arch]}

version=$1
prefix=$2

platform=$(uname)
case ${platform} in
  Linux)
    platform=linux
    ;;
  Darwin)
    platform=macos
    ;;
  MSYS_NT*|MINGW64_NT*)
    platform=windows
    ;;
  *)
    echo "Unsupported platform: ${platform}"
    exit 0
    ;;
esac

mkdir -p ${prefix}
url="https://github.com/Kitware/CMake/releases/download/v${version}/cmake-${version}-${platform}-"
case ${platform} in
  macos)
    url+="universal.tar.gz"
    curl -L ${url} | tar -xzf - --directory ${prefix} --strip-components=1
    ln -s CMake.app/Contents/bin ${prefix}/bin
    ;;
  windows)
    url+="${arch}.zip"
    archive_name=$(basename ${url})
    curl -L -o ${archive_name} ${url}
    unzip ${archive_name}
    base_name=$(basename ${archive_name} .zip)
    mv ${base_name}/* ${prefix}
    rm -rf ${base_name} ${archive_name}
    ;;
  *)
    url+="${arch}.tar.gz"
    curl -L ${url} | tar -xzf - --directory ${prefix} --strip-components=1
    ;;
esac
