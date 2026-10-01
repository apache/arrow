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
       [aarch64]=arm64)

arch=$(uname -m)
if [ -z "${archs[$arch]:-}" ]; then
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

# The binaries are no longer available from https://dl.min.io (HTTP 410),
# so they are fetched from the GitHub releases instead. See GH-47908.
# Use specific versions for minio server and client to avoid CI failures on new releases.
minio_version="RELEASE.2025-01-20T14-49-07Z"
mc_version="RELEASE.2024-09-16T17-43-14Z"

# Hardcoded so that a replaced or tampered release asset is detected. Fetching
# the .sha256sum assets instead would only detect a corrupted download, as they
# are served from the same release.
declare -A minio_checksums
minio_checksums=([linux-amd64]=3439ca54a18f931cb900b35db0fa223f9ef9ab0c872ec63bbd115777863f4f91
                 [linux-arm64]=ca96cbe3ee773aec918319be3d0a9f1566fd0dd3c52b973c97abd030dde84d38
                 [darwin-amd64]=80e7bf28a337313189a8f54403f623f2f1894a672bafb0cc08665d958d8bd1c0
                 [darwin-arm64]=f8469f3eaa868bf21cc09b5a6087cf4997bf63d73979ecd5d3fb8b8358ac3f55
                 [windows-amd64]=ec1bf8de91729ef670abdbc9e743560c4957de251168ce5b48b0ae1154c45d85)
declare -A mc_checksums
mc_checksums=([linux-amd64]=9a9e7d32c175f2804d6880d5ad3623097ea439f0e0304aa6039874d0f0c493d8
              [linux-arm64]=f4a269854283736f46024e73a35a3194f8286067a36877f2aecacf3bf6e41bf0
              [darwin-amd64]=0638adf8be9052fc04a8e08e0df5ab516c11c16a67aa62b14f1c653b46fdd0cc
              [darwin-arm64]=668db3dd797e4f285b33ba652c6de8f8edc4bed31d74d9f17f3c0ab829482c4d
              [windows-amd64]=b2378ff1d04370df15436362cbd6660ceb1a401886659a0b86747cddbe9f8722)

exe_suffix=""
if [ "${platform}" = "windows" ]; then
  exe_suffix=".exe"
fi

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

verify_checksum()
{
  local file=$1
  local expected=$2
  local actual

  if [ -z "${expected}" ]; then
    echo "No known checksum for ${file} on ${platform}-${arch}"
    rm -f "${file}"
    exit 1
  fi
  if type sha256sum > /dev/null 2>&1; then
    actual=$(sha256sum "${file}" | cut -d ' ' -f 1)
  else
    actual=$(shasum --algorithm 256 "${file}" | cut -d ' ' -f 1)
  fi
  if [ "${actual}" != "${expected}" ]; then
    echo "Checksum mismatch for ${file}"
    echo "  expected: ${expected}"
    echo "  actual:   ${actual}"
    rm -f "${file}"
    exit 1
  fi
}

if [[ ! -x ${prefix}/bin/minio ]]; then
  url="https://github.com/minio/minio/releases/download/${minio_version}/minio.${platform}-${arch}.${minio_version}${exe_suffix}"
  echo "Fetching ${url}..."
  download "${prefix}/bin/minio" "${url}"
  verify_checksum "${prefix}/bin/minio" "${minio_checksums[${platform}-${arch}]:-}"
  chmod +x "${prefix}/bin/minio"
fi
if [[ ! -x ${prefix}/bin/mc ]]; then
  url="https://github.com/minio/mc/releases/download/${mc_version}/mc.${platform}-${arch}.${mc_version}${exe_suffix}"
  echo "Fetching ${url}..."
  download "${prefix}/bin/mc" "${url}"
  verify_checksum "${prefix}/bin/mc" "${mc_checksums[${platform}-${arch}]:-}"
  chmod +x "${prefix}/bin/mc"
fi
