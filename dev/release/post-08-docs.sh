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

set -e
set -u

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARROW_DIR="${SOURCE_DIR}/../.."
: ${ARROW_SITE_DIR:="${ARROW_DIR}/../arrow-site"}

if [ "$#" -ne 2  ]; then
  echo "Usage: $0 <version> <previous_version>"
  exit 1
fi

version=$1
previous_version=$2
release_tag="apache-arrow-${version}"
branch_name=release-docs-${version}

case "${version}" in
  *.0.0)
    is_major_release=yes
    ;;
  *)
    is_major_release=no
    ;;
esac

pushd "${ARROW_SITE_DIR}"
source "${SOURCE_DIR}/git-vars.sh"
git fetch --all --prune --tags --force -j$(nproc)
git checkout .
git checkout ${DEFAULT_BRANCH}
git clean -d -f -x
git branch -D asf-site || :
git checkout -b asf-site origin/asf-site
git rebase apache/asf-site
git branch -D ${branch_name} || :
git checkout -b ${branch_name}
# list and remove previous versioned docs
versioned_paths=()
for versioned_path in docs/*.0/; do
  versioned_paths+=(${versioned_path})
  rm -rf ${versioned_path}
done
# add to list and remove dev docs
versioned_paths+=("docs/dev/")
rm -rf docs/dev/
if [ "$is_major_release" = "yes" ] ; then
  # copy the current stable docs to temporary directory
  # (remove java reference to reduce size)
  rm -rf docs/java/reference/
  cp -r docs/ docs_temp/
fi
# delete current stable docs and restore all previous versioned docs
rm -rf docs/*
git checkout "${versioned_paths[@]}"
curl \
  --fail \
  --location \
  --remote-name \
  https://apache.jfrog.io/artifactory/arrow/docs/${version}/docs.tar.gz
tar xvf docs.tar.gz
rm -f docs.tar.gz
git checkout docs/c_glib/index.html
if [ "$is_major_release" = "yes" ] ; then
  previous_series=${previous_version%.*}
  mv docs_temp docs/${previous_series}
fi
git add docs
git commit -m "[Website] Update documentations for ${version}"
git clean -d -f -x
popd

: ${PUSH:=1}

if [ ${PUSH} -gt 0 ]; then
  pushd "${ARROW_SITE_DIR}"
  git push -u origin ${branch_name}
  github_url=$(git remote get-url origin | \
                 sed \
                   -e 's,^git@github.com:,https://github.com/,' \
                   -e 's,\.git$,,')
  popd

  echo "Success!"
  echo "Create a pull request:"
  echo "  ${github_url}/pull/new/${branch_name}"
  echo "Note! Use the 'asf-site' base branch for the PR!"
fi
