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

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 ``<target-directory> [<vcpkg-version> [<vcpkg-ports-patch>]]"
  exit 1
fi

arrow_dir=$(cd -- "$(dirname -- "$0")/../.." && pwd -P)
default_vcpkg_ports_patch="${arrow_dir}/ci/vcpkg/ports.patch"

vcpkg_destination=$1
vcpkg_version=${2:-}
vcpkg_ports_patch=${3:-$default_vcpkg_ports_patch}

if [ -z "${vcpkg_version}" ]; then
  vcpkg_version=$(source "${arrow_dir}/.env" && echo "$VCPKG")
fi

# reduce the fetched data using a shallow clone
git clone --shallow-since=2021-04-01 https://github.com/microsoft/vcpkg ${vcpkg_destination}

pushd ${vcpkg_destination}

git checkout "${vcpkg_version}"

if [[ "${OSTYPE:-}" == "msys" ]]; then
  ./bootstrap-vcpkg.bat -disableMetrics
else
  ./bootstrap-vcpkg.sh -disableMetrics
fi

if [ -f "${vcpkg_ports_patch}" ]; then
  git apply --verbose --ignore-whitespace ${vcpkg_ports_patch}
  echo "Patch successfully applied to the VCPKG port files!"
fi

if [ -n "${GITHUB_TOKEN:-}" ] && [ -n "${GITHUB_ACTOR:-}" ]; then
  if type dnf 2>/dev/null; then
    dnf install -y epel-release
    dnf install -y mono-complete
    wget https://dist.nuget.org/win-x86-commandline/v6.10.0/nuget.exe
    mv nuget.exe /usr/libexec/
    cat <<NUGET > /usr/bin/nuget
#!/bin/sh

exec mono /usr/libexec/nuget.exe "\$@"
NUGET
    chmod +x /usr/bin/nuget
    nuget help
  fi
  nuget_url="https://nuget.pkg.github.com/${GITHUB_ACTOR}/index.json"
  cat <<NUGET_CONFIG > "${VCPKG_ROOT}/nuget.config"
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <config>
    <add key="defaultPushSource" value="${nuget_url}" />
  </config>
  <apikeys>
    <add key="${nuget_url}" value="${GITHUB_TOKEN}" />
  </apikeys>
  <packageSources>
    <clear />
    <add key="GitHubPackages" value="${nuget_url}" />
  </packageSources>
  <packageSourcesCredentials>
    <GitHubPackages>
      <add key="Username" value="${GITHUB_ACTOR}" />
      <add key="Password" value="${GITHUB_TOKEN}" />
    </GitHubPackages>
  </packageSourcesCredentials>
</configuration>
NUGET_CONFIG
fi

popd
