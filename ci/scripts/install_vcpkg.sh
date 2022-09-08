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

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 ``<target-directory> [<vcpkg-version> [<vcpkg-ports-patch>]]"
  exit 1
fi

arrow_dir=$(cd -- "$(dirname -- "$0")/../.." && pwd -P)
default_vcpkg_version=$(cat "${arrow_dir}/.env" | grep "VCPKG" | cut -d "=" -f2 | tr -d '"')
default_vcpkg_ports_patch="${arrow_dir}/ci/vcpkg/ports.patch"

vcpkg_destination=$1
vcpkg_version=${2:-$default_vcpkg_version}
vcpkg_ports_patch=${3:-$default_vcpkg_ports_patch}

# reduce the fetched data using a shallow clone
git clone --shallow-since=2021-04-01 https://github.com/microsoft/vcpkg ${vcpkg_destination}

pushd ${vcpkg_destination}

git checkout "${vcpkg_version}"

if [[ "$OSTYPE" == "msys" ]]; then
  ./bootstrap-vcpkg.bat -disableMetrics
else
  ./bootstrap-vcpkg.sh -disableMetrics
fi

if [ -f "${vcpkg_ports_patch}" ]; then
  git apply --verbose --ignore-whitespace ${vcpkg_ports_patch}
  echo "Patch successfully applied to the VCPKG port files!"
fi

popd
