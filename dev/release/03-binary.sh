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

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <version> <rc-num> <artifact_dir>"
  exit
fi

version=$1
rc=$2
artifact_dir=$3

docker_image_name=apache-arrow/release-binary

# if [ -d tmp/ ]; then
#   echo "Cannot run: tmp/ exists"
#   exit
# fi

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

format_json() {
  docker \
    run \
    --rm \
    --interactive \
    ${docker_image_name} \
    jq .
}

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
      format_json
}

upload_deb() {
  local version=$1
  local distribution=$2
  local code_name=$3

  # TODO
  # apt-ftparchive sources pool > dists/stretch/main/binary-amd64/Sources
  # apt-ftparchive packages pool > dists/stretch/main/binary-amd64/Packages
  # apt-ftparchive contents pool > dists/stretch/main/binary-amd64/Contents
  # apt-ftparchive release dists/stretch/main/binary-amd64/ > dists/stretch/main/binary-amd64/Release
  # gpg --sign --detach-sign --armor --output dists/stretch/main/binary-amd64/Release.gpg dists/stretch/main/binary-amd64/Release
  # gpg --clear-sign --armor --output dists/stretch/main/binary-amd64/InRelease dists/stretch/main/binary-amd64/Release

  # TODO
  # debsign **.{dsc,changes}
  # It should be inline signed.

  bintray \
    POST /packages/apache/arrow/${distribution}-rc/versions \
    --data-binary "
{
  \"name\": \"${version}\",
  \"desc\": \"Apache Arrow ${version}.\"
}
"
  for base_path in *; do
    local path=pool/${code_name}/main/a/apache-arrow/${base_path}
    local sha256=$(shasum -a 256 ${base_path} | awk '{print $1}')
    bintray \
      PUT /content/apache/arrow/${distribution}-rc/${version}/${path} \
      --header "X-Bintray-Publish: 1" \
      --header "X-Bintray-Override: 1" \
      --header "X-Checksum-Sha2: ${sha256}" \
      --data-binary "@${base_path}"
  done
}

apt_ftparchive() {
  local distribution=$1
  shift
  docker \
    run \
    --rm \
    --tty \
    --interactive \
    --volume "$PWD":/host \
    ${docker_image_name} \
    bash -c "cd /host && apt-ftparchive $*"
}

upload_apt() {
  local version=$1
  local distribution=$2

  # local files=$(
  #   bintray \
  #     GET /packages/apache/arrow/${distribution}-rc/versions/${version}/files | \
  #     jq -r ".[].path")

  local tmp_dir=tmp/apt
  # rm -rf ${tmp_dir}
  # mkdir -p ${tmp_dir}
  pushd "${tmp_dir}"
  # for file in ${files}; do
  #   mkdir -p "$(dirname ${file})"
  #   curl \
  #     --fail \
  #     --location \
  #     --output ${file} \
  #     https://dl.bintray.com/apache/arrow/${file}
  # done
  for pool_code_name in pool/*; do
    local code_name=$(basename ${pool_code_name})
    local dist=dists/${code_name}/main
    mkdir -p ${dist}/{source,binary-amd64}
    apt_ftparchive \
      ${distribution} sources pool/${code_name} > \
      dists/${code_name}/main/source/Sources
    apt_ftparchive \
      ${distribution} packages pool/${code_name} > \
      dists/${code_name}/main/binary-amd64/Packages
    apt_ftparchive \
      ${distribution} contents pool/${code_name} > \
      dists/${code_name}/main/Contents-amd64
    # TODO
    apt_ftparchive \
      ${distribution} release dists/${code_name}/main/binary-amd64/ > \
      dists/${code_name}/main/binary-amd64/Release
    gpg \
      --sign \
      --detach-sign \
      --armor \
      --output
  done
  # apt-ftparchive packages pool > dists/stretch/main/binary-amd64/Packages
  # apt-ftparchive contents pool > dists/stretch/main/binary-amd64/Contents
  # apt-ftparchive release dists/stretch/main/binary-amd64/ > dists/stretch/main/binary-amd64/Release
  # gpg --sign --detach-sign --armor --output dists/stretch/main/binary-amd64/Release.gpg dists/stretch/main/binary-amd64/Release
  # gpg --clear-sign --armor --output dists/stretch/main/binary-amd64/InRelease dists/stretch/main/binary-amd64/Release

  popd
  # rm -rf "$tmp_dir"

  # TODO
  # apt-ftparchive sources pool > dists/stretch/main/binary-amd64/Sources
  # apt-ftparchive packages pool > dists/stretch/main/binary-amd64/Packages
  # apt-ftparchive contents pool > dists/stretch/main/binary-amd64/Contents
  # apt-ftparchive release dists/stretch/main/binary-amd64/ > dists/stretch/main/binary-amd64/Release
  # gpg --sign --detach-sign --armor --output dists/stretch/main/binary-amd64/Release.gpg dists/stretch/main/binary-amd64/Release
  # gpg --clear-sign --armor --output dists/stretch/main/binary-amd64/InRelease dists/stretch/main/binary-amd64/Release

  # TODO
  # debsign **.{dsc,changes}
  # It should be inline signed.

#   bintray \
#     POST /packages/apache/arrow/${distribution}-rc/versions \
#     --data-binary "
# {
#   \"name\": \"${version}\",
#   \"desc\": \"Apache Arrow ${version}.\"
# }
# "
#   for base_path in *; do
#     local path=pool/${code_name}/main/a/apache-arrow/${base_path}
#     local sha256=$(shasum -a 256 ${base_path} | awk '{print $1}')
#     bintray \
#       PUT /content/apache/arrow/${distribution}-rc/${version}/${path} \
#       --header "X-Bintray-Publish: 1" \
#       --header "X-Bintray-Override: 1" \
#       --header "X-Checksum-Sha2: ${sha256}" \
#       --data-binary "@${base_path}"
#   done
}

version="0.11.0-rc${rc}"

docker build -t ${docker_image_name} ${SOURCE_DIR}/binary

have_debian=no
have_ubuntu=no
pushd "${artifact_dir}"
for dir in *; do
  is_deb=no
  is_rpm=no
  case "$dir" in
    debian-*)
      distribution=debian
      code_name=$(echo ${dir} | sed -e 's/^debian-//')
      is_deb=yes
      have_debian=yes
      ;;
    ubuntu-*)
      distribution=ubuntu
      code_name=$(echo ${dir} | sed -e 's/^ubuntu-//')
      is_deb=yes
      have_ubuntu=yes
      ;;
  esac

  if [ ${is_deb} = "yes" ]; then
    pushd ${dir}
    : upload_deb ${version} ${distribution} ${code_name}
    popd
  fi
done
popd

if [ ${have_debian} = "yes" ]; then
  upload_apt ${version} debian
fi
# if [ ${have_ubuntu} = "yes" ]; then
#   upload_apt ${version} ubuntu
# fi

echo "Success! The release candidate binaries are available here:"
if [ ${have_debian} = "yes" ]; then
  echo "  https://binray.com/apache/arrow/debian-rc/${version}"
fi
if [ ${have_ubuntu} = "yes" ]; then
  echo "  https://binray.com/apache/arrow/ubuntu-rc/${version}"
fi
