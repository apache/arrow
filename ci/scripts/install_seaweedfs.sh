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

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <prefix>"
  exit 1
fi

# The version is the SeaweedFS release tag without the leading "v" (e.g. 4.31).
version=$1
prefix=$2

declare -A archs
archs=([x86_64]=amd64
       [arm64]=arm64
       [aarch64]=arm64)

arch=$(uname -m)
if [ -z "${archs[$arch]}" ]; then
  echo "Unsupported architecture: ${arch}"
  exit 0
fi
arch=${archs[$arch]}

# SeaweedFS ships statically linked Go binaries, so there is no
# glibc/musl distinction: a single Linux archive works everywhere.
platform=$(uname)
extension=tar.gz
case ${platform} in
  Linux)
    platform=linux
    ;;
  Darwin)
    platform=darwin
    ;;
  MSYS_NT*|MINGW64_NT*)
    platform=windows
    extension=zip
    ;;
  *)
    echo "Unsupported platform: ${platform}"
    exit 0
    ;;
esac

download()
{
  local output=$1
  local url=$2

  mkdir -p "$(dirname "${output}")"
  if type wget > /dev/null 2>&1; then
    wget -nv --output-document "${output}" "${url}"
  else
    curl --fail --location --output "${output}" "${url}"
  fi
}

if [[ ! -x ${prefix}/bin/weed ]]; then
  url="https://github.com/seaweedfs/seaweedfs/releases/download/${version}/${platform}_${arch}.${extension}"
  echo "Fetching ${url}..."
  tmp=$(mktemp -d)
  archive="${tmp}/seaweedfs.${extension}"
  download "${archive}" "${url}"
  if [ "${extension}" = "zip" ]; then
    unzip -q "${archive}" -d "${tmp}"
  else
    tar -xzf "${archive}" -C "${tmp}"
  fi
  mkdir -p "${prefix}/bin"
  mv "${tmp}/weed" "${prefix}/bin/weed"
  chmod +x "${prefix}/bin/weed"
  rm -rf "${tmp}"
fi
