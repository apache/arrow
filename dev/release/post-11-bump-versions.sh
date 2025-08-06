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
#
set -ue

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SOURCE_DIR}/git-vars.sh"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <next_version>"
  exit 1
fi

: ${BUMP_DEFAULT:=1}
: ${BUMP_UPDATE_LOCAL_DEFAULT_BRANCH:=${BUMP_DEFAULT}}
: ${BUMP_VERSION_POST_TAG:=${BUMP_DEFAULT}}
: ${BUMP_DEB_PACKAGE_NAMES:=${BUMP_DEFAULT}}
: ${BUMP_LINUX_PACKAGES:=${BUMP_DEFAULT}}
: ${BUMP_PUSH:=${BUMP_DEFAULT}}
: ${BUMP_TAG:=${BUMP_DEFAULT}}

. $SOURCE_DIR/utils-prepare.sh

version=$1
next_version=$2
next_version_snapshot="${next_version}-SNAPSHOT"
current_version_before_bump="$(current_version)"

case "${version}" in
  *.0.0)
    is_major_release=1
    ;;
  *)
    is_major_release=0
    ;;
esac

if [ ${BUMP_UPDATE_LOCAL_DEFAULT_BRANCH} -gt 0 ]; then
  echo "Updating local default branch"
  case "$(uname)" in
    Linux)
      n_jobs=$(nproc)
      ;;
    Darwin)
      n_jobs=$(sysctl -n hw.ncpu)
      ;;
    *)
      n_jobs=${NPROC:-0} # see git-config, 0 means "reasonable default"
      ;;
  esac

  git fetch --all --prune --tags --force -j"$n_jobs"
  git checkout ${DEFAULT_BRANCH}
  git rebase upstream/${DEFAULT_BRANCH}
fi

if [ ${BUMP_VERSION_POST_TAG} -gt 0 ]; then
  echo "Updating versions for ${next_version_snapshot}"
  update_versions "${version}" "${next_version}" "snapshot"
  git commit -m "MINOR: [Release] Update versions for ${next_version_snapshot}"
fi

if [ ${BUMP_DEB_PACKAGE_NAMES} -gt 0 ] && \
     [ "${next_version}" != "${current_version_before_bump}" ]; then
  update_deb_package_names "${version}" "${next_version}"
fi

if [ ${BUMP_LINUX_PACKAGES} -gt 0 ]; then
  update_linux_packages "${version}" "$(git log -n1 --format=%aI apache-arrow-${version})"
fi

if [ ${BUMP_PUSH} -gt 0 ]; then
  echo "Pushing changes to the default branch in apache/arrow"
  git push upstream ${DEFAULT_BRANCH}
fi

if [ ${BUMP_TAG} -gt 0 -a ${is_major_release} -gt 0 ]; then
  dev_tag=apache-arrow-${next_version}.dev
  echo "Tagging ${dev_tag}"
  git tag ${dev_tag} ${DEFAULT_BRANCH}
  git push upstream ${dev_tag}
fi
