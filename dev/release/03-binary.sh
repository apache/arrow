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

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <version> <rc-num> <gpg-key-id> <artifact-dir>"
  exit
fi

version=$1
rc=$2
gpg_key_id=$3
artifact_dir=$4

docker_image_name=apache-arrow/release-binary

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

if [ -z "${BINTRAY_PASSWORD}" ]; then
  echo "BINTRAY_PASSWORD is empty"
  exit 1
fi

jq() {
  docker \
    run \
    --rm \
    --interactive \
    ${docker_image_name} \
    jq "$@"
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
      jq .
}

ensure_version() {
  local version=$1
  local rc=$2
  local target=$3

  local version_name=${version}-rc${rc}

  if ! bintray \
         GET \
         /packages/apache/arrow/${target}-rc/versions/${version_name}; then
    bintray \
      POST /packages/apache/arrow/${target}-rc/versions \
      --data-binary "
{
  \"name\": \"${version_name}\",
  \"desc\": \"Apache Arrow ${version} RC${rc}.\"
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
      GET /packages/apache/arrow/${target}-rc/versions/${version_name}/files | \
      jq -r ".[].path")

  for file in ${files}; do
    mkdir -p "$(dirname ${file})"
    curl \
      --fail \
      --location \
      --output ${file} \
      https://dl.bintray.com/apache/arrow/${file}
  done
}

upload_file() {
  local version=$1
  local rc=$2
  local target=$3
  local local_path=$4
  local upload_path=$5

  local version_name=${version}-rc${rc}

  local sha256=$(shasum -a 256 ${local_path} | awk '{print $1}')
  bintray \
    PUT /content/apache/arrow/${target}-rc/${version_name}/${target}-rc/${upload_path} \
    --header "X-Bintray-Publish: 1" \
    --header "X-Bintray-Override: 1" \
    --header "X-Checksum-Sha2: ${sha256}" \
    --data-binary "@${local_path}"
}

upload_deb() {
  local version=$1
  local rc=$2
  local distribution=$3
  local code_name=$4

  ensure_version ${version} ${rc} ${distribution}

  for base_path in *; do
    upload_file \
      ${version} \
      ${rc} \
      ${distribution} \
      ${base_path} \
      pool/${code_name}/main/a/apache-arrow/${base_path}
  done
}

apt_ftparchive() {
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

  local tmp_dir=tmp/${distribution}
  rm -rf ${tmp_dir}
  mkdir -p ${tmp_dir}
  pushd "${tmp_dir}"

  download_files ${version} ${rc} ${distribution}

  pushd ${distribution}-rc

  local keyring_name=apache-arrow-keyring.gpg
  rm -f ${keyring_name}
  curl --fail https://dist.apache.org/repos/dist/dev/arrow/KEYS | \
    gpg \
      --no-default-keyring \
      --keyring ./${keyring_name} \
      --import - || : # XXX: Ignore gpg error
  upload_file \
    ${version} \
    ${rc} \
    ${distribution} \
    ${keyring_name} \
    ${keyring_name}

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
      --local-user ${gpg_key_id} \
      --sign \
      --detach-sign \
      --armor \
      --output dists/${code_name}/Release.gpg \
      dists/${code_name}/Release

    for path in $(find dists/${code_name}/ -type f); do
      upload_file \
        ${version} \
        ${rc} \
        ${distribution} \
        ${path} \
        ${path}
    done
  done

  popd

  popd
  rm -rf "$tmp_dir"
}

rpm() {
  local gpg_agent_volume="$(dirname $(gpgconf --list-dir socketdir))"
  docker \
    run \
    --rm \
    --tty \
    --interactive \
    --user $(id -u):$(id -g) \
    --volume "$PWD":/host \
    --volume "${HOME}/.gnupg:/.gnupg:ro" \
    --volume "${gpg_agent_volume}:${gpg_agent_volume}:ro" \
    ${docker_image_name} \
    bash -c "cd /host && rpm $*"
}

upload_rpm() {
  local version=$1
  local rc=$2
  local distribution=$3
  local distribution_version=$4

  local version_name=${version}-rc${rc}

  ensure_version ${version} ${rc} ${distribution}

  for rpm_path in *.rpm; do
    local upload_path=${distribution_version}
    case ${base_path} in
      *.src.rpm)
        upload_path=${upload_path}/Source/SPackages
        ;;
      *)
        upload_path=${upload_path}/x86_64/Packages
        ;;
    esac
    upload_path=${upload_path}/${rpm_path}
    # TODO: Done in crossbow?
    rpm \
      -D "_gpg_name\\ ${gpg_key_id}" \
      --addsign \
      ${rpm_path}
    upload_file \
      ${version} \
      ${rc} \
      ${distribution} \
      ${rpm_path} \
      ${upload_path}
    # TODO: Re-compute checksum and upload
  done
}

createrepo() {
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

  local tmp_dir=tmp/${distribution}
  rm -rf ${tmp_dir}
  mkdir -p ${tmp_dir}
  pushd "${tmp_dir}"

  download_files ${version} ${rc} ${distribution}

  pushd ${distribution}-rc
  local keyring_name=RPM-GPG-KEY-apache-arrow
  curl -o ${keyring_name} https://dist.apache.org/repos/dist/dev/arrow/KEYS
  upload_file \
    ${version} \
    ${rc} \
    ${distribution} \
    ${keyring_name} \
    ${keyring_name}
  for version_dir in $(find . -mindepth 1 -maxdepth 1 -type d); do
    for arch_dir in ${version_dir}/*; do
      mkdir -p ${arch_dir}/repodata/
      createrepo ${arch_dir}
      for repo_path in ${arch_dir}/repodata/*; do
        upload_file \
          ${version} \
          ${rc} \
          ${distribution} \
          ${repo_path} \
          ${repo_path}
      done
    done
  done
  popd

  popd
  rm -rf "$tmp_dir"
}

upload_python() {
  local version=$1
  local rc=$2
  local target=python

  ensure_version ${version} ${rc} ${target}

  for base_path in *; do
    upload_file \
      ${version} \
      ${rc} \
      ${target} \
      ${base_path} \
      ${base_path}
  done
}

docker build -t ${docker_image_name} ${SOURCE_DIR}/binary

have_debian=no
have_ubuntu=no
have_centos=no
have_python=no
pushd "${artifact_dir}"
for dir in *; do
  is_deb=no
  is_rpm=no
  is_python=no
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
    conda-*|wheel-*)
      is_python=yes
      have_python=yes
      ;;
  esac

  if [ ${is_deb} = "yes" ]; then
    pushd ${dir}
    upload_deb ${version} ${rc} ${distribution} ${code_name}
    popd
  elif [ ${is_rpm} = "yes" ]; then
    pushd ${dir}
    upload_rpm ${version} ${rc} ${distribution} ${distribution_version}
    popd
  elif [ ${is_python} = "yes" ]; then
    pushd ${dir}
    upload_python ${version} ${rc}
    popd
  fi
done
popd

if [ ${have_debian} = "yes" ]; then
  upload_apt ${version} ${rc} debian
fi
if [ ${have_ubuntu} = "yes" ]; then
  upload_apt ${version} ${rc} ubuntu
fi
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
if [ ${have_python} = "yes" ]; then
  echo "  https://binray.com/apache/arrow/python-rc/${version}-rc${rc}"
fi

# Debian/Ubuntu:
# % sudo apt install -y -V lsb-release apt-transport-https
# % sudo wget -O /usr/share/keyrings/apache-arrow-keyring.gpg https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-keyring.gpg
# % sudo tee /etc/apt/sources.list.d/apache-arrow.list <<APT_LINE
# deb [signed-by=/usr/share/keyrings/apache-arrow-keyring.gpg] https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/ $(lsb_release --codename --short) main
# deb-src [signed-by=/usr/share/keyrings/red-data-tools-keyring.gpg] https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/ $(lsb_release --codename --short) main
# APT_LINE
# % sudo apt update
#
# CentOS:
# % sudo tee /etc/yum.repos.d/Apache-Arrow.repo <<REPO
# [apache-arrow]
# name=Apache Arrow
# baseurl=https://dl.bintray.com/apache/arrow/centos/\$releasever/\$basearch/
# gpgcheck=1
# enabled=1
# gpgkey=https://dl.bintray.com/apache/arrow/centos/RPM-GPG-KEY-apache-arrow
# REPO
