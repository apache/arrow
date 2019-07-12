#!/bin/bash
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
set -e
set -o pipefail

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

version=$1
rc=$2

if [ -z "${BINTRAY_PASSWORD}" ]; then
  echo "BINTRAY_PASSWORD is empty"
  exit 1
fi

if ! jq --help > /dev/null 2>&1; then
  echo "jq is required"
  exit 1
fi

: ${BINTRAY_REPOSITORY:=apache/arrow}

docker_image_name=apache-arrow/release-binary

bintray() {
  local command=$1
  shift
  local path=$1
  shift
  local url=https://bintray.com/api/v1${path}
  echo "${command} ${url}" 1>&2
  curl \
    --fail \
    --basic \
    --user "${BINTRAY_USER:-$USER}:${BINTRAY_PASSWORD}" \
    --header "Content-Type: application/json" \
    --request ${command} \
    ${url} \
    "$@" | \
      jq .
}

ensure_version() {
  local version=$1
  local target=$2

  if ! bintray \
         GET \
         /packages/${BINTRAY_REPOSITORY}/${target}/versions/${version}; then
    bintray \
      POST /packages/${BINTRAY_REPOSITORY}/${target}/versions \
      --data-binary "
{
  \"name\": \"${version}\",
  \"desc\": \"Apache Arrow ${version}.\"
}
"
  fi
}

download_files() {
  local version=$1
  local rc=$2
  local target=$3

  local version_name=${version}-rc${rc}

  local files=$(
    bintray \
      GET /packages/${BINTRAY_REPOSITORY}/${target}-rc/versions/${version_name}/files | \
      jq -r ".[].path")

  local file
  for file in ${files}; do
    mkdir -p "$(dirname ${file})"
    curl \
      --fail \
      --location \
      --output ${file} \
      https://dl.bintray.com/${BINTRAY_REPOSITORY}/${file}
  done
}

delete_file() {
  local version=$1
  local target=$2
  local path=$3

  bintray \
    DELETE /content/${BINTRAY_REPOSITORY}/${target}/${path}
}

upload_file() {
  local version=$1
  local target=$2
  local path=$3

  local sha256=$(shasum -a 256 ${path} | awk '{print $1}')
  local request_path=/content/${BINTRAY_REPOSITORY}/${target}/${version}/${target}/${path}
  if ! bintray \
         PUT ${request_path} \
         --header "X-Bintray-Publish: 1" \
         --header "X-Bintray-Override: 1" \
         --header "X-Checksum-Sha2: ${sha256}" \
         --data-binary "@${path}"; then
    delete_file ${version} ${target} ${path}
    bintray \
      PUT ${request_path} \
      --header "X-Bintray-Publish: 1" \
      --header "X-Bintray-Override: 1" \
      --header "X-Checksum-Sha2: ${sha256}" \
      --data-binary "@${path}"
  fi
}

docker build -t ${docker_image_name} ${SOURCE_DIR}/binary

for target in debian ubuntu centos python; do
  tmp_dir=tmp/${target}
  rm -rf ${tmp_dir}
  mkdir -p ${tmp_dir}
  pushd ${tmp_dir}
  ensure_version ${version} ${target}
  download_files ${version} ${rc} ${target}
  mv ${target}-rc ${target}
  pushd ${target}
  if [ ${target} = "python" ]; then
    mv ${version}-rc${rc} ${version}
  fi
  for file in $(find . -type f); do
    upload_file ${version} ${target} ${file}
  done
  popd
  popd
  rm -rf ${tmp_dir}
done

echo "Success! The release binaries are available here:"
echo "  https://bintray.com/${BINTRAY_REPOSITORY}/debian/${version}"
echo "  https://bintray.com/${BINTRAY_REPOSITORY}/ubuntu/${version}"
echo "  https://bintray.com/${BINTRAY_REPOSITORY}/centos/${version}"
echo "  https://bintray.com/${BINTRAY_REPOSITORY}/python/${version}"
