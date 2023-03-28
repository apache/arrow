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
  git fetch --all --prune --tags --force -j$(nproc)
  git checkout ${DEFAULT_BRANCH}
  git rebase apache/${DEFAULT_BRANCH}
fi

if [ ${BUMP_VERSION_POST_TAG} -gt 0 ]; then
  echo "Updating versions for ${next_version_snapshot}"
  update_versions "${version}" "${next_version}" "snapshot"
  git commit -m "MINOR: [Release] Update versions for ${next_version_snapshot}"
fi

if [ ${BUMP_DEB_PACKAGE_NAMES} -gt 0 ]; then
  echo "Updating .deb package names for ${next_version}"
  so_version() {
    local version=$1
    local major_version=$(echo $version | sed -E -e 's/^([0-9]+)\.[0-9]+\.[0-9]+$/\1/')
    local minor_version=$(echo $version | sed -E -e 's/^[0-9]+\.([0-9]+)\.[0-9]+$/\1/')
    expr ${major_version} \* 100 + ${minor_version}
  }
  deb_lib_suffix=$(so_version $version)
  next_deb_lib_suffix=$(so_version $next_version)
  if [ "${deb_lib_suffix}" != "${next_deb_lib_suffix}" ]; then
    cd $SOURCE_DIR/../tasks/linux-packages/apache-arrow
    for target in debian*/lib*${deb_lib_suffix}.install; do
      git mv \
        ${target} \
        $(echo $target | sed -e "s/${deb_lib_suffix}/${next_deb_lib_suffix}/")
    done
    deb_lib_suffix_substitute_pattern="s/(lib(arrow|gandiva|parquet)[-a-z]*)${deb_lib_suffix}/\\1${next_deb_lib_suffix}/g"
    sed -i.bak -E -e "${deb_lib_suffix_substitute_pattern}" debian*/control*
    rm -f debian*/control*.bak
    git add debian*/control*
    cd -
    cd $SOURCE_DIR/../tasks/
    sed -i.bak -E -e "${deb_lib_suffix_substitute_pattern}" tasks.yml
    rm -f tasks.yml.bak
    git add tasks.yml
    cd -
    cd $SOURCE_DIR
    sed -i.bak -E -e "${deb_lib_suffix_substitute_pattern}" rat_exclude_files.txt
    rm -f rat_exclude_files.txt.bak
    git add rat_exclude_files.txt
    git commit -m "MINOR: [Release] Update .deb package names for $next_version"
    cd -
  fi
fi

if [ ${BUMP_LINUX_PACKAGES} -gt 0 ]; then
  echo "Updating .deb/.rpm changelogs for $version"
  cd $SOURCE_DIR/../tasks/linux-packages
  rake \
    version:update \
    ARROW_RELEASE_TIME="$(git log -n1 --format=%aI apache-arrow-${version})" \
    ARROW_VERSION=${version}
  git add */debian*/changelog */yum/*.spec.in
  git commit -m "MINOR: [Release] Update .deb/.rpm changelogs for $version"
  cd -
fi

if [ ${BUMP_PUSH} -gt 0 ]; then
  echo "Pushing changes to the default branch in apache/arrow"
  git push apache ${DEFAULT_BRANCH}
fi

if [ ${BUMP_TAG} -gt 0 -a ${is_major_release} -gt 0 ]; then
  dev_tag=apache-arrow-${next_version}.dev
  echo "Tagging ${dev_tag}"
  git tag ${dev_tag} ${DEFAULT_BRANCH}
  git push apache ${dev_tag}
fi
