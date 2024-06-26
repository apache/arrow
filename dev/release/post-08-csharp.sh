#!/usr/bin/env bash
# -*- indent-tabs-mode: nil; sh-indentation: 2; sh-basic-offset: 2 -*-
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
#

set -eux

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <version>"
  exit
fi

version=$1

if [ -z "${NUGET_API_KEY}" ]; then
  echo "NUGET_API_KEY is empty"
  exit 1
fi

base_names=()
base_names+=(Apache.Arrow.${version})
base_names+=(Apache.Arrow.Flight.${version})
base_names+=(Apache.Arrow.Flight.AspNetCore.${version})
base_names+=(Apache.Arrow.Compression.${version})
for base_name in ${base_names[@]}; do
  for extension in nupkg snupkg; do
    path=${base_name}.${extension}
    rm -f ${path}
    curl \
      --fail \
      --location \
      --remote-name \
      https://apache.jfrog.io/artifactory/arrow/nuget/${version}/${path}
  done
  dotnet nuget push \
    ${base_name}.nupkg \
    -k ${NUGET_API_KEY} \
    -s https://api.nuget.org/v3/index.json
  rm -f ${base_name}.{nupkg,snupkg}
done

echo "Success! The released NuGet package is available here:"
echo "  https://www.nuget.org/packages/Apache.Arrow/${version}"
