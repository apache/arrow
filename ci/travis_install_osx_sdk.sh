#!/usr/bin/env bash

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

export MACOSX_DEPLOYMENT_TARGET="10.9"
export CONDA_BUILD_SYSROOT="$(xcode-select -p)/Platforms/MacOSX.platform/Developer/SDKs/MacOSX${MACOSX_DEPLOYMENT_TARGET}.sdk"

if [[ ! -d ${CONDA_BUILD_SYSROOT} || "$OSX_FORCE_SDK_DOWNLOAD" == "1" ]]; then
  echo "downloading ${macosx_deployment_target} sdk"
  curl -L -O https://github.com/phracker/MacOSX-SDKs/releases/download/10.13/MacOSX${MACOSX_DEPLOYMENT_TARGET}.sdk.tar.xz
  tar -xf MacOSX${MACOSX_DEPLOYMENT_TARGET}.sdk.tar.xz -C "$(dirname "$CONDA_BUILD_SYSROOT")"
  # set minimum sdk version to our target
  plutil -replace MinimumSDKVersion -string ${MACOSX_DEPLOYMENT_TARGET} $(xcode-select -p)/Platforms/MacOSX.platform/Info.plist
  plutil -replace DTSDKName -string macosx${MACOSX_DEPLOYMENT_TARGET}internal $(xcode-select -p)/Platforms/MacOSX.platform/Info.plist
fi

if [ -d "${CONDA_BUILD_SYSROOT}" ]; then
  echo "Found CONDA_BUILD_SYSROOT: ${CONDA_BUILD_SYSROOT}"
else
  echo "Missing CONDA_BUILD_SYSROOT: ${CONDA_BUILD_SYSROOT}"
  exit 1
fi

