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

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <vcpkg version> <target directory>"
  exit 1
fi

vcpkg_version=$1
vcpkg_destination=$2
vcpkg_patch=$(realpath $(dirname "${0}")/../vcpkg/ports.patch)

git clone --depth 1 --branch ${vcpkg_version} https://github.com/microsoft/vcpkg ${vcpkg_destination}

pushd ${vcpkg_destination}

./bootstrap-vcpkg.sh -useSystemBinaries -disableMetrics
git apply --ignore-whitespace ${vcpkg_patch}
echo "Patch successfully applied!"

popd
