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

declare -A linux_archs
linux_archs=([x86_64]="x86_64"
             [aarch64]="aarch64")

declare -A windows_archs
windows_archs=([64-bit]="x86_64"
               [ARM 64-bit Processor]="arm64")

version=$1
prefix=$2

platform=$(uname)
case ${platform} in
  Linux)
    platform=linux
    arch=$(uname -m)
    if [ -z "${linux_archs[$arch]}" ]; then
      echo "Unsupported architecture on Linux: ${arch}"
      exit 0
    fi
    arch=${linux_archs[$arch]}
    ;;
  Darwin)
    platform=macos
    arch=universal
    ;;
  MSYS_NT*|MINGW64_NT*)
    platform=windows
    arch=$(powershell -Command "(Get-CimInstance Win32_OperatingSystem).OSArchitecture")
    if [ -z "${windows_archs[$arch]}" ]; then
      echo "Unsupported architecture on Windows: ${arch}"
      exit 0
    fi
    arch=${windows_archs[$arch]}
    ;;
  *)
    echo "Unsupported platform: ${platform}"
    exit 0
    ;;
esac

mkdir -p "${prefix}"
url="https://github.com/Kitware/CMake/releases/download/v${version}/cmake-${version}-${platform}-"
case ${platform} in
  windows)
    url+="${arch}.zip"
    archive_name=$(basename "${url}")
    curl -L -o "${archive_name}" "${url}"
    unzip "${archive_name}"
    base_name=$(basename "${archive_name}" .zip)
    cp -a "${base_name}"/* "${prefix}"
    rm -rf "${base_name}" "${archive_name}"
    ;;
  *)
    url+="${arch}.tar.gz"
    curl -L "${url}" | tar -xzf - --directory "${prefix}" --strip-components=1
    if [ "${platform}" = "macos" ]; then
      ln -s CMake.app/Contents/bin "${prefix}/bin"
    fi
    ;;
esac
