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

set -eu

: ${SOURCE_DEFAULT:=1}
: ${SOURCE_DOWNLOAD:=${SOURCE_DEFAULT}}
: ${SOURCE_RAT:=${SOURCE_DEFAULT}}
: ${SOURCE_UPLOAD:=${SOURCE_DEFAULT}}
: ${SOURCE_PR:=${SOURCE_DEFAULT}}

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

version=$1
rc=$2

. "${SOURCE_DIR}/utils-env.sh"

tag=apache-arrow-${version}-rc${rc}
maint_branch=maint-${version}
rc_branch="release-${version}-rc${rc}"
rc_url="https://dist.apache.org/repos/dist/dev/arrow/${tag}"

echo "Preparing source for tag ${tag}"

: ${release_hash:=$(cd "${SOURCE_TOP_DIR}" && git rev-list --max-count=1 ${tag})}
: ${GITHUB_REPOSITORY:=apache/arrow}

if [ ${SOURCE_UPLOAD} -gt 0 ]; then
  if [ -z "$release_hash" ]; then
    echo "Cannot continue: unknown git tag: $tag"
    exit
  fi
fi

echo "Using commit $release_hash"

tarball=apache-arrow-${version}.tar.gz

if [ ${SOURCE_DOWNLOAD} -gt 0 ]; then
  # Wait for the release candidate workflow to finish before attempting
  # to download the tarball from the GitHub Release.
  . $SOURCE_DIR/utils-watch-gh-workflow.sh ${tag} "release_candidate.yml"
  rm -rf artifacts
  gh release download ${tag} \
    --dir artifacts \
    --repo "${GITHUB_REPOSITORY}"
fi

if [ ${SOURCE_RAT} -gt 0 ]; then
  "${SOURCE_DIR}/run-rat.sh" artifacts/${tarball}
fi

if [ ${SOURCE_UPLOAD} -gt 0 ]; then
  # check out the arrow RC folder
  svn co --depth=empty https://dist.apache.org/repos/dist/dev/arrow tmp

  # add the release candidate for the tag
  mkdir -p tmp/${tag}

  # copy the release candidate tarball and related files into the tmp dir
  cp artifacts/${tarball}* tmp/${tag}

  # commit to svn
  svn add tmp/${tag}
  svn ci -m "Apache Arrow ${version} RC${rc}" tmp/${tag}

  # clean up
  rm -rf artifacts
  rm -rf tmp

  echo "Success! The release candidate is available here:"
  echo "  ${rc_url}"
  echo ""
  echo "Commit SHA1: ${release_hash}"
  echo ""
fi

# Create Pull Request and Crossbow comment to run verify source tasks
if [ ${SOURCE_PR} -gt 0 ]; then
  archery crossbow \
    verify-release-candidate \
    --base-branch=${maint_branch} \
    --create-pr \
    --head-branch=${rc_branch} \
    --pr-body="PR to verify Release Candidate" \
    --pr-title="WIP: [Release] Verify ${rc_branch}" \
    --remote=https://github.com/${GITHUB_REPOSITORY} \
    --rc=${rc} \
    --verify-source \
    --version=${version}
fi
