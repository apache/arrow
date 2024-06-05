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

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <storage-testbench version>"
  exit 1
fi

case "$(uname -m)" in
  aarch64|arm64|x86_64)
    : # OK
    ;;
  *)
    echo "GCS testbench is installed only on x86 or arm architectures: $(uname -m)"
    exit 0
    ;;
esac

# On newer pythons install into the system will fail, so override that
export PIP_BREAK_SYSTEM_PACKAGES=1

version=$1
if [[ "${version}" -eq "default" ]]; then
  version="v0.39.0"
  # Latests versions of Testbench require newer setuptools
  ${PYTHON:-python3} -m pip install --upgrade setuptools
fi

${PYTHON:-python3} -m pip install \
  "https://github.com/googleapis/storage-testbench/archive/${version}.tar.gz"
