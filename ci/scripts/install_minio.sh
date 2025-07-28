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

version=$1
prefix=$2

declare -A archs
archs=([x86_64]=amd64
       [arm64]=arm64
       [aarch64]=arm64
       [s390x]=s390x)

arch=$(uname -m)
if [ -z "${archs[$arch]}" ]; then
  echo "Unsupported architecture: ${arch}"
  exit 0
fi
arch=${archs[$arch]}

platform=$(uname)
case ${platform} in
  Linux)
    platform=linux
    ;;
  Darwin)
    platform=darwin
    ;;
  MSYS_NT*|MINGW64_NT*)
    platform=windows
    ;;
  *)
    echo "Unsupported platform: ${platform}"
    exit 0
    ;;
esac

if [ "${version}" != "latest" ]; then
  echo "Cannot fetch specific versions of minio, only latest is supported."
  exit 1
fi

# Use specific versions for minio server and client to avoid CI failures on new releases.
minio_version="minio.RELEASE.2025-01-20T14-49-07Z"
mc_version="mc.RELEASE.2024-09-16T17-43-14Z"

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

if [[ ! -x ${prefix}/bin/minio ]]; then
  url="https://dl.min.io/server/minio/release/${platform}-${arch}/archive/${minio_version}"
  echo "Fetching ${url}..."
  download "${prefix}/bin/minio" "${url}"
  chmod +x "${prefix}/bin/minio"
fi
if [[ ! -x ${prefix}/bin/mc ]]; then
  url="https://dl.min.io/client/mc/release/${platform}-${arch}/archive/${mc_version}"
  echo "Fetching ${url}..."
  download "${prefix}/bin/mc" "${url}"
  chmod +x "${prefix}/bin/mc"
fi
