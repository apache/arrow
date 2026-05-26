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

set -ex

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

version=$1
if [[ "${version}" = "default" ]]; then
  version="v0.55.0"
fi

# The Python to install pipx with
: "${PIPX_BASE_PYTHON:=$(which python3)}"
# The Python to install the GCS testbench with
: "${PIPX_PYTHON:=${PIPX_BASE_PYTHON:-$(which python3)}}"

export PIP_BREAK_SYSTEM_PACKAGES=1
${PIPX_BASE_PYTHON} -m pip install -U pipx

pipx_flags=(--verbose --python "${PIPX_PYTHON}")
if [[ $(id -un) == "root" ]]; then
  # Install globally as /root/.local/bin is typically not in $PATH
  pipx_flags+=(--global)
fi
if [[ -n "${PIPX_PIP_ARGS}" ]]; then
  pipx_flags+=(--pip-args "'${PIPX_PIP_ARGS}'")
fi
${PIPX_BASE_PYTHON} -m pipx install "${pipx_flags[@]}" \
  "https://github.com/googleapis/storage-testbench/archive/${version}.tar.gz"
