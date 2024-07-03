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

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <version> <next_version> <rc-num>"
  exit 1
fi

. $SOURCE_DIR/utils-prepare.sh

version=$1
next_version=$2
next_version_snapshot="${next_version}-SNAPSHOT"
rc_number=$3

release_candidate_tag="apache-arrow-${version}-rc${rc_number}"
release_branch="release-${version}"
release_candidate_branch="release-${version}-rc${rc_number}"

: ${PREPARE_DEFAULT:=1}
: ${PREPARE_CHANGELOG:=${PREPARE_DEFAULT}}
: ${PREPARE_DEB_PACKAGE_NAMES:=${PREPARE_DEFAULT}}
: ${PREPARE_LINUX_PACKAGES:=${PREPARE_DEFAULT}}
: ${PREPARE_VERSION_PRE_TAG:=${PREPARE_DEFAULT}}
: ${PREPARE_BRANCH:=${PREPARE_DEFAULT}}
: ${PREPARE_TAG:=${PREPARE_DEFAULT}}

if [ ${PREPARE_TAG} -gt 0 ]; then
  if [ $(git tag -l "${release_candidate_tag}") ]; then
    echo "Delete existing git tag $release_candidate_tag"
    git tag -d "${release_candidate_tag}"
  fi
fi

if [ ${PREPARE_BRANCH} -gt 0 ]; then
  if [[ $(git branch -l "${release_candidate_branch}") ]]; then
    next_rc_number=$(($rc_number+1))
    source "${SOURCE_DIR}/git-vars.sh"
    echo "Branch ${release_candidate_branch} already exists, so create a new release candidate:"
    echo "1. Checkout the default branch for major releases and maint-<version> for patch releases."
    echo "2. Execute the script again with bumped RC number."
    echo "Commands:"
    echo "   git checkout ${DEFAULT_BRANCH}"
    echo "   dev/release/01-prepare.sh ${version} ${next_version} ${next_rc_number}"
    exit 1
  fi

  echo "Create local branch ${release_candidate_branch} for release candidate ${rc_number}"
  git checkout -b ${release_candidate_branch}
fi

############################## Pre-Tag Commits ##############################

if [ ${PREPARE_CHANGELOG} -gt 0 ]; then
  echo "Updating changelog for $version"
  # Update changelog
  archery release changelog add $version
  git add ${SOURCE_DIR}/../../CHANGELOG.md
  git commit -m "MINOR: [Release] Update CHANGELOG.md for $version"
fi

if [ ${PREPARE_DEB_PACKAGE_NAMES} -gt 0 ]; then
  update_deb_package_names "$(current_version)" "${version}"
fi

if [ ${PREPARE_LINUX_PACKAGES} -gt 0 ]; then
  update_linux_packages "${version}" "$(date +%Y-%m-%dT%H:%M:%S%z)"
fi

if [ ${PREPARE_VERSION_PRE_TAG} -gt 0 ]; then
  echo "Prepare release ${version} on tag ${release_candidate_tag} then reset to version ${next_version_snapshot}"

  update_versions "${version}" "${next_version}" "release"
  git commit -m "MINOR: [Release] Update versions for ${version}"
fi

############################## Tag the Release ##############################

if [ ${PREPARE_TAG} -gt 0 ]; then
  git tag -a "${release_candidate_tag}" -m "[Release] Apache Arrow Release ${version} RC${rc_number}"
fi
