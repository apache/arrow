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
set -u
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
gpg_agent_extra_socket="$(gpgconf --list-dirs agent-extra-socket)"
if [ $(uname) = "Darwin" ]; then
  docker_uid=10000
  docker_gid=10000
else
  docker_uid=$(id -u)
  docker_gid=$(id -g)
fi
docker_ssh_key="${SOURCE_DIR}/binary/id_rsa"

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

if ! jq --help > /dev/null 2>&1; then
  echo "jq is required"
  exit 1
fi

: ${BINTRAY_REPOSITORY:=apache/arrow}
: ${SOURCE_BINTRAY_REPOSITORY:=${BINTRAY_REPOSITORY}}

BINTRAY_DOWNLOAD_URL_BASE=https://dl.bintray.com

docker_run() {
  docker \
    run \
    --rm \
    --tty \
    --interactive \
    --user ${docker_uid}:${docker_gid} \
    --volume "$PWD":/host \
    ${docker_image_name} \
    bash -c "cd /host && $*"
}

docker_gpg_ssh() {
  local ssh_port=$1
  shift
  local known_hosts_file=$(mktemp -t "arrow-binary-gpg-ssh-known-hosts.XXXXX")
  local exit_code=
  if ssh \
      -o StrictHostKeyChecking=no \
      -o UserKnownHostsFile=${known_hosts_file} \
      -i "${docker_ssh_key}" \
      -p ${ssh_port} \
      -R "/home/arrow/.gnupg/S.gpg-agent:${gpg_agent_extra_socket}" \
      arrow@127.0.0.1 \
      "$@"; then
    exit_code=$?;
  else
    exit_code=$?;
  fi
  rm -f ${known_hosts_file}
  return ${exit_code}
}

docker_run_gpg_ready() {
  local container_id_dir=$(mktemp -d -t "arrow-binary-gpg-container.XXXXX")
  local container_id_file=${container_id_dir}/id
  docker \
    run \
    --rm \
    --detach \
    --cidfile ${container_id_file} \
    --publish-all \
    --volume "$PWD":/host \
    ${docker_image_name} \
    bash -c "
if [ \$(id -u) -ne ${docker_uid} ]; then
  usermod --uid ${docker_uid} arrow
  chown -R arrow: ~arrow
fi
/usr/sbin/sshd -D
"
  local container_id=$(cat ${container_id_file})
  local ssh_port=$(docker port ${container_id} | grep -E -o '[0-9]+$')
  # Wait for sshd available
  while ! docker_gpg_ssh ${ssh_port} : > /dev/null 2>&1; do
    sleep 0.1
  done
  gpg --export ${gpg_key_id} | docker_gpg_ssh ${ssh_port} gpg --import
  docker_gpg_ssh ${ssh_port} "cd /host && $@"
  docker kill ${container_id}
  rm -rf ${container_id_dir}
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
         /packages/${BINTRAY_REPOSITORY}/${target}-rc/versions/${version_name}; then
    bintray \
      POST /packages/${BINTRAY_REPOSITORY}/${target}-rc/versions \
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
      GET /packages/${SOURCE_BINTRAY_REPOSITORY}/${target}-rc/versions/${version_name}/files | \
      jq -r ".[].path")

  local file=
  for file in ${files}; do
    mkdir -p "$(dirname ${file})"
    curl \
      --fail \
      --location \
      --output ${file} \
      ${BINTRAY_DOWNLOAD_URL_BASE}/${SOURCE_BINTRAY_REPOSITORY}/${file} &
  done
}

delete_file() {
  local version=$1
  local rc=$2
  local target=$3
  local upload_path=$4

  local version_name=${version}-rc${rc}

  bintray \
    DELETE /content/${BINTRAY_REPOSITORY}/${target}-rc/${upload_path}
}

upload_file() {
  local version=$1
  local rc=$2
  local target=$3
  local local_path=$4
  local upload_path=$5

  local version_name=${version}-rc${rc}

  local sha256=$(shasum -a 256 ${local_path} | awk '{print $1}')
  local request_path=/content/${BINTRAY_REPOSITORY}/${target}-rc/${version_name}/${target}-rc/${upload_path}
  if ! bintray \
         PUT ${request_path} \
         --header "X-Bintray-Publish: 1" \
         --header "X-Bintray-Override: 1" \
         --header "X-Checksum-Sha2: ${sha256}" \
         --data-binary "@${local_path}"; then
    delete_file ${version} ${rc} ${target} ${upload_path}
    bintray \
      PUT ${request_path} \
      --header "X-Bintray-Publish: 1" \
      --header "X-Bintray-Override: 1" \
      --header "X-Checksum-Sha2: ${sha256}" \
      --data-binary "@${local_path}"
  fi
}

sign_and_upload_file() {
  local version=$1
  local rc=$2
  local target=$3
  local local_path=$4
  local upload_path=$5

  local sha256=$(shasum -a 256 ${local_path} | awk '{print $1}')
  local download_path=/${BINTRAY_REPOSITORY}/${target}-rc/${upload_path}
  if curl \
       --fail \
       --head \
       ${BINTRAY_DOWNLOAD_URL_BASE}${download_path} | \
         grep -q "^X-Checksum-Sha2: ${sha256}"; then
    return 0
  fi

  upload_file ${version} ${rc} ${target} ${local_path} ${upload_path}

  local suffix=
  for suffix in asc sha256 sha512; do
    pushd $(dirname ${local_path})
    local local_path_base=$(basename ${local_path})
    local output_dir=$(mktemp -d -t "arrow-binary-sign.XXXXX")
    local output=${output_dir}/tmp.${suffix}
    case $suffix in
      asc)
        gpg \
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
    upload_file ${version} ${rc} ${target} ${output} ${upload_path}.${suffix}
    rm -rf ${output_dir}
    popd
  done
}

upload_deb_file() {
  local version=$1
  local rc=$2
  local distribution=$3
  local code_name=$4
  local base_path=$5

  case ${base_path} in
    *.dsc|*.changes)
      docker_run_gpg_ready debsign -k${gpg_key_id} --re-sign ${base_path}
      ;;
    *.asc|*.sha256|*.sha512)
      return
      ;;
  esac
  sign_and_upload_file \
    ${version} \
    ${rc} \
    ${distribution} \
    ${base_path} \
    pool/${code_name}/main/a/apache-arrow/${base_path}
}

upload_deb() {
  local version=$1
  local rc=$2
  local distribution=$3
  local code_name=$4

  local base_path=
  for base_path in *; do
    upload_deb_file ${version} ${rc} ${distribution} ${code_name} ${base_path} &
  done
  wait
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
  wait

  pushd ${distribution}-rc

  local keyring_name=apache-arrow-keyring.gpg
  rm -f ${keyring_name}
  curl --fail https://dist.apache.org/repos/dist/dev/arrow/KEYS | \
    gpg \
      --no-default-keyring \
      --keyring ./${keyring_name} \
      --import - || : # XXX: Ignore gpg error
  sign_and_upload_file \
    ${version} \
    ${rc} \
    ${distribution} \
    ${keyring_name} \
    ${keyring_name} &

  local pool_code_name=
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

    local path=
    for path in $(find dists/${code_name}/ -type f); do
      sign_and_upload_file \
        ${version} \
        ${rc} \
        ${distribution} \
        ${path} \
        ${path} &
    done
    wait
  done
  popd

  popd
  rm -rf ${tmp_dir}
}

upload_rpm_file() {
  local version=$1
  local rc=$2
  local distribution=$3
  local distribution_version=$4
  local rpm_path=$5

  local upload_path=${distribution_version}

  case ${rpm_path} in
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
  sign_and_upload_file \
    ${version} \
    ${rc} \
    ${distribution} \
    ${rpm_path} \
    ${upload_path}
}

upload_rpm() {
  local version=$1
  local rc=$2
  local distribution=$3
  local distribution_version=$4

  local rpm_path=
  for rpm_path in *.rpm; do
    upload_rpm_file \
      ${version} \
      ${rc} \
      ${distribution} \
      ${distribution_version} \
      ${rpm_path} &
  done
  wait
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
  wait

  pushd ${distribution}-rc
  local keyring_name=RPM-GPG-KEY-apache-arrow
  curl -o ${keyring_name} https://dist.apache.org/repos/dist/dev/arrow/KEYS
  sign_and_upload_file \
    ${version} \
    ${rc} \
    ${distribution} \
    ${keyring_name} \
    ${keyring_name} &
  local version_dir=
  local arch_dir=
  local repo_path=
  for version_dir in $(find . -mindepth 1 -maxdepth 1 -type d); do
    for arch_dir in ${version_dir}/*; do
      mkdir -p ${arch_dir}/repodata/
      docker_run createrepo ${arch_dir}
      for repo_path in ${arch_dir}/repodata/*; do
        sign_and_upload_file \
          ${version} \
          ${rc} \
          ${distribution} \
          ${repo_path} \
          ${repo_path} &
      done
    done
  done
  wait
  popd

  popd
  rm -rf ${tmp_dir}
}

upload_python() {
  local version=$1
  local rc=$2
  local target=python

  local base_path=
  for base_path in *; do
    case ${base_path} in
      *.asc|*.sha256|*.sha512)
        continue
        ;;
    esac
    sign_and_upload_file \
      ${version} \
      ${rc} \
      ${target} \
      ${base_path} \
      ${version}-rc${rc}/${base_path} &
  done
  wait
}

docker build -t ${docker_image_name} ${SOURCE_DIR}/binary

chmod go-rwx "${docker_ssh_key}"

# By default upload all artifacts.
# To deactivate one category, deactivate the category and all of its dependents.
# To explicitly select one category, set UPLOAD_DEFAULT=0 UPLOAD_X=1.
: ${UPLOAD_DEFAULT:=1}
: ${UPLOAD_CENTOS_RPM:=${UPLOAD_DEFAULT}}
: ${UPLOAD_CENTOS_YUM:=${UPLOAD_DEFAULT}}
: ${UPLOAD_DEBIAN_APT:=${UPLOAD_DEFAULT}}
: ${UPLOAD_DEBIAN_DEB:=${UPLOAD_DEFAULT}}
: ${UPLOAD_PYTHON:=${UPLOAD_DEFAULT}}
: ${UPLOAD_UBUNTU_APT:=${UPLOAD_DEFAULT}}
: ${UPLOAD_UBUNTU_DEB:=${UPLOAD_DEFAULT}}

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
    case ${distribution} in
      debian)
        if [ ${UPLOAD_DEBIAN_DEB} -gt 0 ]; then
          ensure_version ${version} ${rc} ${distribution}
          upload_deb ${version} ${rc} ${distribution} ${code_name} &
        fi
        ;;
      ubuntu)
        if [ ${UPLOAD_UBUNTU_DEB} -gt 0 ]; then
          ensure_version ${version} ${rc} ${distribution}
          upload_deb ${version} ${rc} ${distribution} ${code_name} &
        fi
        ;;
    esac
    popd
  elif [ ${is_rpm} = "yes" ]; then
    pushd ${dir}
    if [ ${UPLOAD_CENTOS_RPM} -gt 0 ]; then
      ensure_version ${version} ${rc} ${distribution}
      upload_rpm ${version} ${rc} ${distribution} ${distribution_version} &
    fi
    popd
  elif [ ${is_python} = "yes" ]; then
    pushd ${dir}
    if [ ${UPLOAD_PYTHON} -gt 0 ]; then
      ensure_version ${version} ${rc} python
      upload_python ${version} ${rc} &
    fi
    popd
  fi
done
wait
popd

if [ ${have_debian} = "yes" ]; then
  if [ ${UPLOAD_DEBIAN_APT} -gt 0 ]; then
    ensure_version ${version} ${rc} debian
    upload_apt ${version} ${rc} debian
  fi
fi
if [ ${have_ubuntu} = "yes" ]; then
  if [ ${UPLOAD_UBUNTU_APT} -gt 0 ]; then
    ensure_version ${version} ${rc} ubuntu
    upload_apt ${version} ${rc} ubuntu
  fi
fi
if [ ${have_centos} = "yes" ]; then
  if [ ${UPLOAD_CENTOS_YUM} -gt 0 ]; then
    ensure_version ${version} ${rc} centos
    upload_yum ${version} ${rc} centos
  fi
fi

echo "Success! The release candidate binaries are available here:"
if [ ${have_debian} = "yes" ]; then
  echo "  https://bintray.com/${BINTRAY_REPOSITORY}/debian-rc/${version}-rc${rc}"
fi
if [ ${have_ubuntu} = "yes" ]; then
  echo "  https://bintray.com/${BINTRAY_REPOSITORY}/ubuntu-rc/${version}-rc${rc}"
fi
if [ ${have_centos} = "yes" ]; then
  echo "  https://bintray.com/${BINTRAY_REPOSITORY}/centos-rc/${version}-rc${rc}"
fi
if [ ${have_python} = "yes" ]; then
  echo "  https://bintray.com/${BINTRAY_REPOSITORY}/python-rc/${version}-rc${rc}"
fi
