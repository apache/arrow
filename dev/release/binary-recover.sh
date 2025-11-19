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
set -o pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -ne 0 ]; then
  echo "Usage: $0"
  exit
fi

cd "${SOURCE_DIR}"

if [ ! -f .env ]; then
  echo "You must create $(pwd)/.env"
  echo "You can use $(pwd)/.env.example as template"
  exit 1
fi
# shellcheck source=SCRIPTDIR/.env.example
. .env

. utils-binary.sh

# By default recover all artifacts.
# To deactivate one category, deactivate the category and all of its dependents.
# To explicitly select one category, set RECOVER_DEFAULT=0 RECOVER_X=1.
: "${RECOVER_DEFAULT:=1}"
: "${RECOVER_DEBIAN:=${RECOVER_DEFAULT}}"
: "${RECOVER_UBUNTU:=${RECOVER_DEFAULT}}"

rake_tasks=()
apt_targets=()
if [ "${RECOVER_DEBIAN}" -gt 0 ]; then
  rake_tasks+=(apt:recover)
  apt_targets+=(debian)
fi
if [ "${RECOVER_UBUNTU}" -gt 0 ]; then
  rake_tasks+=(apt:recover)
  apt_targets+=(ubuntu)
fi

tmp_dir=binary/tmp
mkdir -p "${tmp_dir}"

docker_run \
  ./runner.sh \
  rake \
  --trace \
  "${rake_tasks[@]}" \
  APT_TARGETS="$(
    IFS=,
    echo "${apt_targets[*]}"
  )" \
  ARTIFACTORY_API_KEY="${ARTIFACTORY_API_KEY}" \
  ARTIFACTS_DIR="${tmp_dir}/artifacts" \
  RC="" \
  STAGING="${STAGING:-no}" \
  VERBOSE="${VERBOSE:-no}" \
  VERSION=""
