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
archs=([x86_64]=amd64
       [arm64]=arm64
       [aarch64]=arm64
       [s390x]=s390x)

declare -A platforms
platforms=([Linux]=linux
           [Darwin]=darwin)

arch=$(uname -m)
platform=$(uname)
version=$1
prefix=$2

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <prefix>"
  exit 1
elif [ -z ${archs[$arch]} ]; then
  echo "Unsupported architecture: ${arch}"
  exit 0
elif [ -z ${platforms[$platform]} ]; then
  echo "Unsupported platform: ${platform}"
  exit 0
elif [ "${version}" != "latest" ]; then
  echo "Cannot fetch specific versions of minio, only latest is supported."
  exit 1
fi

arch=${archs[$arch]}
platform=${platforms[$platform]}

# Use specific versions for minio server and client to avoid CI failures on new releases.
minio_version="minio.RELEASE.2022-05-26T05-48-41Z"
mc_version="mc.RELEASE.2022-05-09T04-08-26Z"

if [[ ! -x ${prefix}/bin/minio ]]; then
  url="https://dl.min.io/server/minio/release/${platform}-${arch}/archive/${minio_version}"
  echo "Fetching ${url}..."
  wget -nv --output-document ${prefix}/bin/minio ${url}
  chmod +x ${prefix}/bin/minio
fi
if [[ ! -x ${prefix}/bin/mc ]]; then
  url="https://dl.min.io/client/mc/release/${platform}-${arch}/archive/${mc_version}"
  echo "Fetching ${url}..."
  wget -nv --output-document ${prefix}/bin/mc ${url}
  chmod +x ${prefix}/bin/mc
fi
