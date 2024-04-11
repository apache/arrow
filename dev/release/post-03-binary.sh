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

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

version=$1
rc=$2

cd "${SOURCE_DIR}"

if [ ! -f .env ]; then
  echo "You must create $(pwd)/.env"
  echo "You can use $(pwd)/.env.example as template"
  exit 1
fi
. .env

. utils-binary.sh

# By default deploy all artifacts.
# To deactivate one category, deactivate the category and all of its dependents.
# To explicitly select one category, set DEPLOY_DEFAULT=0 DEPLOY_X=1.
: ${DEPLOY_DEFAULT:=1}
: ${DEPLOY_ALMALINUX:=${DEPLOY_DEFAULT}}
: ${DEPLOY_AMAZON_LINUX:=${DEPLOY_DEFAULT}}
: ${DEPLOY_CENTOS:=${DEPLOY_DEFAULT}}
: ${DEPLOY_DEBIAN:=${DEPLOY_DEFAULT}}
: ${DEPLOY_DOCS:=${DEPLOY_DEFAULT}}
: ${DEPLOY_NUGET:=${DEPLOY_DEFAULT}}
: ${DEPLOY_PYTHON:=${DEPLOY_DEFAULT}}
: ${DEPLOY_R:=${DEPLOY_DEFAULT}}
: ${DEPLOY_UBUNTU:=${DEPLOY_DEFAULT}}

rake_tasks=()
apt_targets=()
yum_targets=()
if [ ${DEPLOY_ALMALINUX} -gt 0 ]; then
  rake_tasks+=(yum:release)
  yum_targets+=(almalinux)
fi
if [ ${DEPLOY_AMAZON_LINUX} -gt 0 ]; then
  rake_tasks+=(yum:release)
  yum_targets+=(amazon-linux)
fi
if [ ${DEPLOY_CENTOS} -gt 0 ]; then
  rake_tasks+=(yum:release)
  yum_targets+=(centos)
fi
if [ ${DEPLOY_DEBIAN} -gt 0 ]; then
  rake_tasks+=(apt:release)
  apt_targets+=(debian)
fi
if [ ${DEPLOY_DOCS} -gt 0 ]; then
  rake_tasks+=(docs:release)
fi
if [ ${DEPLOY_NUGET} -gt 0 ]; then
  rake_tasks+=(nuget:release)
fi
if [ ${DEPLOY_PYTHON} -gt 0 ]; then
  rake_tasks+=(python:release)
fi
if [ ${DEPLOY_R} -gt 0 ]; then
  rake_tasks+=(r:release)
fi
if [ ${DEPLOY_UBUNTU} -gt 0 ]; then
  rake_tasks+=(apt:release)
  apt_targets+=(ubuntu)
fi
rake_tasks+=(summary:release)

tmp_dir=binary/tmp
mkdir -p "${tmp_dir}"

docker_run \
  ./runner.sh \
  rake \
    --trace \
    "${rake_tasks[@]}" \
    APT_TARGETS=$(IFS=,; echo "${apt_targets[*]}") \
    ARTIFACTORY_API_KEY="${ARTIFACTORY_API_KEY}" \
    ARTIFACTS_DIR="${tmp_dir}/artifacts" \
    RC=${rc} \
    STAGING=${STAGING:-no} \
    VERBOSE=${VERBOSE:-no} \
    VERSION=${version} \
    YUM_TARGETS=$(IFS=,; echo "${yum_targets[*]}")
