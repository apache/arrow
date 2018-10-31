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

docker_run() {
  local uid=$(id -u)
  local gid=$(id -g)
  docker \
    run \
    --rm \
    --tty \
    --interactive \
    --user ${uid}:${gid} \
    --volume "$PWD":/host \
    ${docker_image_name} \
    bash -c "cd /host && $*"
}

docker_run_gpg_ready() {
  local gpg_agent_socket_dir="$(gpgconf --list-dir socketdir)"
  local uid=$(id -u)
  local gid=$(id -g)
  local commands="groupadd --gid ${gid} ${USER}"
  commands="${commands} && useradd --uid ${uid} --gid ${gid} ${USER}"
  commands="${commands} && chown ${USER}: /run/user/${uid}"
  commands="${commands} && cd /host"
  commands="${commands} && sudo -u ${USER} -H $*"
  docker \
    run \
    --rm \
    --tty \
    --interactive \
    --volume "$PWD":/host \
    --volume "${HOME}/.gnupg:/home/${USER}/.gnupg:ro" \
    --volume "${gpg_agent_socket_dir}:/run/user/${uid}/gnupg:ro" \
    ${docker_image_name} \
    bash -c "${commands}"
}

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

delete_file() {
  local version=$1
  local rc=$2
  local target=$3
  local upload_path=$4

  local version_name=${version}-rc${rc}

  bintray \
    DELETE /content/apache/arrow/${target}-rc/${upload_path}
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

replace_file() {
  local version=$1
  local rc=$2
  local target=$3
  local local_path=$4
  local upload_path=$5

  # Ignore error
  delete_file ${version} ${rc} ${target} ${upload_path} || :
  upload_file ${version} ${rc} ${target} ${local_path} ${upload_path}

  for suffix in asc sha256 sha512; do
    pushd $(dirname ${local_path})
    local local_path_base=$(basename ${local_path})
    local output=tmp.${suffix}
    case $suffix in
      asc)
        docker_run_gpg_ready gpg \
          --local-user ${gpg_key_id} \
          --detach-sig \
          --output ${output} \
          ${local_path_base}
        ;;
      sha*)
        shasum \
          --algorithm $(echo $suffix | sed -e 's/^sha//') \
          ${local_path_base} > ${output}
        ;;
    esac
    # Ignore error
    delete_file ${version} ${rc} ${target} ${upload_path}.${suffix} || :
    upload_file ${version} ${rc} ${target} ${output} ${upload_path}.${suffix}
    rm -f ${output}
    popd
  done
}

upload_deb() {
  local version=$1
  local rc=$2
  local distribution=$3
  local code_name=$4

  ensure_version ${version} ${rc} ${distribution}

  for base_path in *; do
    case ${base_path} in
      *.dsc|*.changes)
        docker_run_gpg_ready debsign -k${gpg_key_id} ${base_path}
        ;;
      *.asc|*.sha256|*.sha512)
        continue
        ;;
    esac
    replace_file \
      ${version} \
      ${rc} \
      ${distribution} \
      ${base_path} \
      pool/${code_name}/main/a/apache-arrow/${base_path}
  done
}

upload_apt() {
  local version=$1
  local rc=$2
  local distribution=$3

  local tmp_dir=tmp/${distribution}
  rm -rf ${tmp_dir}
  mkdir -p ${tmp_dir}
  pushd ${tmp_dir}

  download_files ${version} ${rc} ${distribution}

  pushd ${distribution}-rc

  local keyring_name=apache-arrow-keyring.gpg
  rm -f ${keyring_name}
  curl --fail https://dist.apache.org/repos/dist/dev/arrow/KEYS | \
    gpg \
      --no-default-keyring \
      --keyring ./${keyring_name} \
      --import - || : # XXX: Ignore gpg error
  replace_file \
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
    docker_run apt-ftparchive \
      sources pool/${code_name} > \
      dists/${code_name}/main/source/Sources
    gzip --keep dists/${code_name}/main/source/Sources
    xz --keep dists/${code_name}/main/source/Sources
    docker_run apt-ftparchive \
      packages pool/${code_name} > \
      dists/${code_name}/main/binary-amd64/Packages
    gzip --keep dists/${code_name}/main/binary-amd64/Packages
    xz --keep dists/${code_name}/main/binary-amd64/Packages
    docker_run apt-ftparchive \
      contents pool/${code_name} > \
      dists/${code_name}/main/Contents-amd64
    gzip --keep dists/${code_name}/main/Contents-amd64
    docker_run apt-ftparchive \
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
      replace_file \
        ${version} \
        ${rc} \
        ${distribution} \
        ${path} \
        ${path}
    done
  done

  popd

  popd
  rm -rf ${tmp_dir}
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
    docker_run_gpg_ready rpm \
      -D "_gpg_name\\ ${gpg_key_id}" \
      --addsign \
      ${rpm_path}
    replace_file \
      ${version} \
      ${rc} \
      ${distribution} \
      ${rpm_path} \
      ${upload_path}
  done
}

upload_yum() {
  local version=$1
  local rc=$2
  local distribution=$3

  local version_name=${version}-rc${rc}

  local tmp_dir=tmp/${distribution}
  rm -rf ${tmp_dir}
  mkdir -p ${tmp_dir}
  pushd ${tmp_dir}

  download_files ${version} ${rc} ${distribution}

  pushd ${distribution}-rc
  local keyring_name=RPM-GPG-KEY-apache-arrow
  curl -o ${keyring_name} https://dist.apache.org/repos/dist/dev/arrow/KEYS
  replace_file \
    ${version} \
    ${rc} \
    ${distribution} \
    ${keyring_name} \
    ${keyring_name}
  for version_dir in $(find . -mindepth 1 -maxdepth 1 -type d); do
    for arch_dir in ${version_dir}/*; do
      mkdir -p ${arch_dir}/repodata/
      docker_run createrepo ${arch_dir}
      for repo_path in ${arch_dir}/repodata/*; do
        replace_file \
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
  rm -rf ${tmp_dir}
}

upload_python() {
  local version=$1
  local rc=$2
  local target=python

  ensure_version ${version} ${rc} ${target}

  for base_path in *; do
    case ${base_path} in
      *.asc|*.sha256|*.sha512)
        continue
        ;;
    esac
    replace_file \
      ${version} \
      ${rc} \
      ${target} \
      ${base_path} \
      ${version}-rc${rc}/${base_path}
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
  case ${dir} in
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
  echo "  https://bintray.com/apache/arrow/debian-rc/${version}-rc${rc}"
fi
if [ ${have_ubuntu} = "yes" ]; then
  echo "  https://bintray.com/apache/arrow/ubuntu-rc/${version}-rc${rc}"
fi
if [ ${have_centos} = "yes" ]; then
  echo "  https://bintray.com/apache/arrow/centos-rc/${version}-rc${rc}"
fi
if [ ${have_python} = "yes" ]; then
  echo "  https://bintray.com/apache/arrow/python-rc/${version}-rc${rc}"
fi
