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
  local rc=$2
  local distribution=$3
  local code_name=$4

  local version_name=${version}-rc${rc}

  bintray \
    POST /packages/apache/arrow/${distribution}-rc/versions \
    --data-binary "
{
  \"name\": \"${version_name}\",
  \"desc\": \"Apache Arrow ${version} RC${rc}.\"
}
"
  local keyring_name=apache-arrow-keyring.gpg
  rm -f ${keyring_name}
  curl https://dist.apache.org/repos/dist/dev/arrow/KEYS | \
    gpg \
      --no-default-keyring \
      --keyring ${keyring_name} \
      --import -
  for base_path in *; do
    local path=pool/${code_name}/main/a/apache-arrow/${base_path}
    local sha256=$(shasum -a 256 ${base_path} | awk '{print $1}')
    bintray \
      PUT /content/apache/arrow/${distribution}-rc/${version_name}/${distribution}-rc/${path} \
      --header "X-Bintray-Publish: 1" \
      --header "X-Bintray-Override: 1" \
      --header "X-Checksum-Sha2: ${sha256}" \
      --data-binary "@${base_path}"
  done
}

apt_ftparchive() {
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
  local rc=$2
  local distribution=$3

  local version_name=${version}-rc${rc}

  local files=$(
    bintray \
      GET /packages/apache/arrow/${distribution}-rc/versions/${version_name}/files | \
      jq -r ".[].path")

  local tmp_dir=tmp/${distribution}
  rm -rf ${tmp_dir}
  mkdir -p ${tmp_dir}
  pushd "${tmp_dir}"

  for file in ${files}; do
    mkdir -p "$(dirname ${file})"
    curl \
      --fail \
      --location \
      --output ${file} \
      https://dl.bintray.com/apache/arrow/${file}
  done

  pushd ${distribution}-rc
  for pool_code_name in pool/*; do
    local code_name=$(basename ${pool_code_name})
    local dist=dists/${code_name}/main
    rm -rf dists
    mkdir -p ${dist}/{source,binary-amd64}
    apt_ftparchive \
      sources pool/${code_name} > \
      dists/${code_name}/main/source/Sources
    gzip --keep dists/${code_name}/main/source/Sources
    xz --keep dists/${code_name}/main/source/Sources
    apt_ftparchive \
      packages pool/${code_name} > \
      dists/${code_name}/main/binary-amd64/Packages
    gzip --keep dists/${code_name}/main/binary-amd64/Packages
    xz --keep dists/${code_name}/main/binary-amd64/Packages
    apt_ftparchive \
      contents pool/${code_name} > \
      dists/${code_name}/main/Contents-amd64
    gzip --keep dists/${code_name}/main/Contents-amd64
    apt_ftparchive \
      release \
      -o "APT::FTPArchive::Release::Origin=Apache\\ Arrow" \
      -o "APT::FTPArchive::Release::Label=Apache\\ Arrow" \
      -o APT::FTPArchive::Release::Codename=${code_name} \
      -o APT::FTPArchive::Release::Architectures=amd64 \
      -o APT::FTPArchive::Release::Components=main \
      dists/${code_name} > \
      dists/${code_name}/Release
    gpg \
      --sign \
      --detach-sign \
      --armor \
      --output dists/${code_name}/Release.gpg \
      dists/${code_name}/Release

    for base_path in $(find dists/${code_name}/ -type f); do
      local path=${distribution}-rc/${base_path}
      local sha256=$(shasum -a 256 ${base_path} | awk '{print $1}')
      bintray \
        PUT /content/apache/arrow/${distribution}-rc/${version_name}/${path} \
        --header "X-Bintray-Publish: 1" \
        --header "X-Bintray-Override: 1" \
        --header "X-Checksum-Sha2: ${sha256}" \
        --data-binary "@${base_path}"
    done
  done
  popd

  popd
  rm -rf "$tmp_dir"
}

upload_rpm() {
  local version=$1
  local rc=$2
  local distribution=$3
  local distribution_version=$4

  local version_name=${version}-rc${rc}

  bintray \
    POST /packages/apache/arrow/${distribution}-rc/versions \
    --data-binary "
{
  \"name\": \"${version_name}\",
  \"desc\": \"Apache Arrow ${version} RC${rc}.\"
}
"
  local keyring_name=RPM-GPG-KEY-Apache-Arrow
  curl -o ${keyring_name} https://dist.apache.org/repos/dist/dev/arrow/KEYS
  for base_path in *; do
    local path=${distribution_version}
    case ${base_path} in
      *.src.rpm*)
        path=${path}/Source/SPackages
        ;;
      *)
        path=${path}/x86_64/Packages
        ;;
    esac
    path=${path}/${base_path}
    local sha256=$(shasum -a 256 ${base_path} | awk '{print $1}')
    bintray \
      PUT /content/apache/arrow/${distribution}-rc/${version_name}/${distribution}-rc/${path} \
      --header "X-Bintray-Publish: 1" \
      --header "X-Bintray-Override: 1" \
      --header "X-Checksum-Sha2: ${sha256}" \
      --data-binary "@${base_path}"
  done
}

createrepo() {
  shift
  docker \
    run \
    --rm \
    --tty \
    --interactive \
    --volume "$PWD":/host \
    ${docker_image_name} \
    bash -c "cd /host && createrepo $*"
}

upload_yum() {
  local version=$1
  local rc=$2
  local distribution=$3

  local version_name=${version}-rc${rc}

  local files=$(
    bintray \
      GET /packages/apache/arrow/${distribution}-rc/versions/${version_name}/files | \
      jq -r ".[].path")

  local tmp_dir=tmp/${distribution}
  rm -rf ${tmp_dir}
  mkdir -p ${tmp_dir}
  pushd "${tmp_dir}"

  for file in ${files}; do
    mkdir -p "$(dirname ${file})"
    curl \
      --fail \
      --location \
      --output ${file} \
      https://dl.bintray.com/apache/arrow/${file}
  done

  pushd ${distribution}-rc
  for version_dir in $(find . -mindepth 1 -maxdepth 1 -type d); do
    for arch_dir in ${version_dir}/*; do
      # TODO
      # rpm --addsign ${arch_dir}/**/*.rpm
      createrepo ${arch_dir}
      for base_path in ${arch_dir}/repodata/*; do
        local path=${distribution}-rc/${base_path}
        local sha256=$(shasum -a 256 ${base_path} | awk '{print $1}')
        bintray \
          PUT /content/apache/arrow/${distribution}-rc/${version_name}/${path} \
          --header "X-Bintray-Publish: 1" \
          --header "X-Bintray-Override: 1" \
          --header "X-Checksum-Sha2: ${sha256}" \
          --data-binary "@${base_path}"
      done
    done
  done
  popd

  popd
  rm -rf "$tmp_dir"
}

docker build -t ${docker_image_name} ${SOURCE_DIR}/binary

have_debian=no
have_ubuntu=no
have_centos=no
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
    centos-*)
      distribution=centos
      distribution_version=$(echo ${dir} | sed -e 's/^centos-//')
      is_rpm=yes
      have_centos=yes
      ;;
  esac

  if [ ${is_deb} = "yes" ]; then
    pushd ${dir}
    : upload_deb ${version} ${rc} ${distribution} ${code_name}
    popd
  elif [ ${is_rpm} = "yes" ]; then
    pushd ${dir}
    : upload_rpm ${version} ${rc} ${distribution} ${distribution_version}
    popd
  fi
done
popd

# if [ ${have_debian} = "yes" ]; then
#   upload_apt ${version} ${rc} debian
# fi
# if [ ${have_ubuntu} = "yes" ]; then
#   upload_apt ${version} ${rc} ubuntu
# fi
if [ ${have_centos} = "yes" ]; then
  upload_yum ${version} ${rc} centos
fi

echo "Success! The release candidate binaries are available here:"
if [ ${have_debian} = "yes" ]; then
  echo "  https://binray.com/apache/arrow/debian-rc/${version}-rc${rc}"
fi
if [ ${have_ubuntu} = "yes" ]; then
  echo "  https://binray.com/apache/arrow/ubuntu-rc/${version}-rc${rc}"
fi
if [ ${have_centos} = "yes" ]; then
  echo "  https://binray.com/apache/arrow/centos-rc/${version}-rc${rc}"
fi

# % sudo apt install -y -V lsb-release apt-transport-https
# % sudo wget -O /usr/share/keyrings/apache-arrow-keyring.gpg https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-keyring.gpg
# % sudo tee /etc/apt/sources.list.d/apache-arrow.list <<APT_LINE
# deb [signed-by=/usr/share/keyrings/apache-arrow-keyring.gpg] https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/ $(lsb_release --codename --short) main
# deb-src [signed-by=/usr/share/keyrings/red-data-tools-keyring.gpg] https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/ $(lsb_release --codename --short) main
# APT_LINE
# % sudo apt update
