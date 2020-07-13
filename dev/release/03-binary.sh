#!/bin/bash
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

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <version> <rc-num> <artifact-dir>"
  exit
fi

version=$1
rc=$2
artifact_dir=$3

if [ -z "$artifact_dir" ]; then
  echo "artifact_dir is empty"
  exit 1
fi

if [ ! -e "$artifact_dir" ]; then
  echo "$artifact_dir does not exist"
  exit 1
fi

if [ ! -d "$artifact_dir" ]; then
  echo "$artifact_dir is not a directory"
  exit 1
fi

artifact_dir="$(pwd)/${artifact_dir}"

cd "${SOURCE_DIR}"

: ${BINTRAY_REPOSITORY_CUSTOM:=${BINTRAY_REPOSITORY:-}}
: ${SOURCE_BINTRAY_REPOSITORY_CUSTOM:=${SOURCE_BINTRAY_REPOSITORY:-}}}

if [ ! -f .env ]; then
  echo "You must create $(pwd)/.env"
  echo "You can use $(pwd)/.env.example as template"
  exit 1
fi
. .env

if [ -n "${BINTRAY_REPOSITORY_CUSTOM}" ]; then
  BINTRAY_REPOSITORY=${BINTRAY_REPOSITORY_CUSTOM}
fi

if [ -n "${SOURCE_BINTRAY_REPOSITORY_CUSTOM}" ]; then
  SOURCE_BINTRAY_REPOSITORY=${SOURCE_BINTRAY_REPOSITORY_CUSTOM}
fi

. binary-common.sh

# By default upload all artifacts.
# To deactivate one category, deactivate the category and all of its dependents.
# To explicitly select one category, set UPLOAD_DEFAULT=0 UPLOAD_X=1.
: ${UPLOAD_DEFAULT:=1}
: ${UPLOAD_CENTOS_RPM:=${UPLOAD_DEFAULT}}
: ${UPLOAD_CENTOS_YUM:=${UPLOAD_DEFAULT}}
: ${UPLOAD_DEBIAN_APT:=${UPLOAD_DEFAULT}}
: ${UPLOAD_DEBIAN_DEB:=${UPLOAD_DEFAULT}}
: ${UPLOAD_NUGET:=${UPLOAD_DEFAULT}}
: ${UPLOAD_PYTHON:=${UPLOAD_DEFAULT}}
: ${UPLOAD_UBUNTU_APT:=${UPLOAD_DEFAULT}}
: ${UPLOAD_UBUNTU_DEB:=${UPLOAD_DEFAULT}}

rake_tasks=()
apt_targets=()
yum_targets=()
if [ ${UPLOAD_DEBIAN_DEB} -gt 0 ]; then
  rake_tasks+=(deb)
  apt_targets+=(debian)
fi
if [ ${UPLOAD_DEBIAN_APT} -gt 0 ]; then
  rake_tasks+=(apt:rc)
  apt_targets+=(debian)
fi
if [ ${UPLOAD_UBUNTU_DEB} -gt 0 ]; then
  rake_tasks+=(deb)
  apt_targets+=(ubuntu)
fi
if [ ${UPLOAD_UBUNTU_APT} -gt 0 ]; then
  rake_tasks+=(apt:rc)
  apt_targets+=(ubuntu)
fi
if [ ${UPLOAD_CENTOS_RPM} -gt 0 ]; then
  rake_tasks+=(rpm)
  yum_targets+=(centos)
fi
if [ ${UPLOAD_CENTOS_YUM} -gt 0 ]; then
  rake_tasks+=(yum:rc)
  yum_targets+=(centos)
fi
if [ ${UPLOAD_NUGET} -gt 0 ]; then
  rake_tasks+=(nuget:rc)
fi
if [ ${UPLOAD_PYTHON} -gt 0 ]; then
  rake_tasks+=(python:rc)
fi
rake_tasks+=(summary:rc)

tmp_dir=binary/tmp
mkdir -p "${tmp_dir}"
source_artifacts_dir="${tmp_dir}/artifacts"
rm -rf "${source_artifacts_dir}"
cp -a "${artifact_dir}" "${source_artifacts_dir}"

docker_run \
  ./runner.sh \
  rake \
    "${rake_tasks[@]}" \
    APT_TARGETS=$(IFS=,; echo "${apt_targets[*]}") \
    ARTIFACTS_DIR="${tmp_dir}/artifacts" \
    BINTRAY_REPOSITORY=${BINTRAY_REPOSITORY} \
    RC=${rc} \
    SOURCE_BINTRAY_REPOSITORY=${SOURCE_BINTRAY_REPOSITORY} \
    VERSION=${version} \
    YUM_TARGETS=$(IFS=,; echo "${yum_targets[*]}")
