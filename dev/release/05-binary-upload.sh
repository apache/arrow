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
set -u
set -o pipefail

export LANG=C
export LC_CTYPE=C

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

version=$1
rc=$2

version_with_rc="${version}-rc${rc}"
crossbow_job_prefix="release-${version_with_rc}"
crossbow_package_dir="${SOURCE_DIR}/../../packages"

: ${CROSSBOW_JOB_NUMBER:="0"}
: ${CROSSBOW_JOB_ID:="${crossbow_job_prefix}-${CROSSBOW_JOB_NUMBER}"}
: ${ARROW_ARTIFACTS_DIR:="${crossbow_package_dir}/${CROSSBOW_JOB_ID}"}

if [ ! -e "${ARROW_ARTIFACTS_DIR}" ]; then
  echo "${ARROW_ARTIFACTS_DIR} does not exist"
  exit 1
fi

if [ ! -d "${ARROW_ARTIFACTS_DIR}" ]; then
  echo "${ARROW_ARTIFACTS_DIR} is not a directory"
  exit 1
fi

cd "${SOURCE_DIR}"

if [ ! -f .env ]; then
  echo "You must create $(pwd)/.env"
  echo "You can use $(pwd)/.env.example as template"
  exit 1
fi
. .env

. utils-binary.sh

# By default upload all artifacts.
# To deactivate one category, deactivate the category and all of its dependents.
# To explicitly select one category, set UPLOAD_DEFAULT=0 UPLOAD_X=1.
: ${UPLOAD_DEFAULT:=1}
: ${UPLOAD_ALMALINUX:=${UPLOAD_DEFAULT}}
: ${UPLOAD_AMAZON_LINUX:=${UPLOAD_DEFAULT}}
: ${UPLOAD_CENTOS:=${UPLOAD_DEFAULT}}
: ${UPLOAD_DEBIAN:=${UPLOAD_DEFAULT}}
: ${UPLOAD_DOCS:=${UPLOAD_DEFAULT}}
: ${UPLOAD_NUGET:=${UPLOAD_DEFAULT}}
: ${UPLOAD_PYTHON:=${UPLOAD_DEFAULT}}
: ${UPLOAD_R:=${UPLOAD_DEFAULT}}
: ${UPLOAD_UBUNTU:=${UPLOAD_DEFAULT}}

rake_tasks=()
apt_targets=()
yum_targets=()
if [ ${UPLOAD_ALMALINUX} -gt 0 ]; then
  rake_tasks+=(yum:rc)
  yum_targets+=(almalinux)
fi
if [ ${UPLOAD_AMAZON_LINUX} -gt 0 ]; then
  rake_tasks+=(yum:rc)
  yum_targets+=(amazon-linux)
fi
if [ ${UPLOAD_CENTOS} -gt 0 ]; then
  rake_tasks+=(yum:rc)
  yum_targets+=(centos)
fi
if [ ${UPLOAD_DEBIAN} -gt 0 ]; then
  rake_tasks+=(apt:rc)
  apt_targets+=(debian)
fi
if [ ${UPLOAD_DOCS} -gt 0 ]; then
  rake_tasks+=(docs:rc)
fi
if [ ${UPLOAD_NUGET} -gt 0 ]; then
  rake_tasks+=(nuget:rc)
fi
if [ ${UPLOAD_PYTHON} -gt 0 ]; then
  rake_tasks+=(python:rc)
fi
if [ ${UPLOAD_R} -gt 0 ]; then
  rake_tasks+=(r:rc)
fi
if [ ${UPLOAD_UBUNTU} -gt 0 ]; then
  rake_tasks+=(apt:rc)
  apt_targets+=(ubuntu)
fi
rake_tasks+=(summary:rc)

tmp_dir=binary/tmp
mkdir -p "${tmp_dir}"
source_artifacts_dir="${tmp_dir}/artifacts"
rm -rf "${source_artifacts_dir}"
cp -a "${ARROW_ARTIFACTS_DIR}" "${source_artifacts_dir}"

docker_run \
  ./runner.sh \
  rake \
    "${rake_tasks[@]}" \
    APT_TARGETS=$(IFS=,; echo "${apt_targets[*]}") \
    ARTIFACTORY_API_KEY="${ARTIFACTORY_API_KEY}" \
    ARTIFACTS_DIR="${tmp_dir}/artifacts" \
    DEB_PACKAGE_NAME=${DEB_PACKAGE_NAME:-} \
    DRY_RUN=${DRY_RUN:-no} \
    GPG_KEY_ID="${GPG_KEY_ID}" \
    RC=${rc} \
    STAGING=${STAGING:-no} \
    VERBOSE=${VERBOSE:-no} \
    VERSION=${version} \
    YUM_TARGETS=$(IFS=,; echo "${yum_targets[*]}")
