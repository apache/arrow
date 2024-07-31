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

set -ue

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <conan-io/conan-center-index's fork repository>"
  exit 1
fi

version=$1
repository=$2

if [ ! -d "${repository}" ]; then
  echo "conan-io/conan-center-index's fork repository doesn't exist: ${repository}"
  exit 1
fi

cd "${repository}"
if [ ! -d .git ]; then
  echo "not a Git repository: ${repository}"
  exit 1
fi

if ! git remote | grep -q '^upstream$'; then
  echo "'upstream' remote doesn't exist: ${repository}"
  echo "Run the following command line in ${repository}:"
  echo "  git remote add upstream https://github.com/conan-io/conan-center-index.git"
  exit 1
fi

echo "Updating repository: ${repository}"
git fetch --all --prune --tags --force
git checkout master
git rebase upstream/master

branch="arrow-${version}"
echo "Creating branch: ${branch}"
git branch -D ${branch} || :
git checkout -b ${branch}

recipes_arrow=recipes/arrow
echo "Updating: ${recipes_arrow}"
tar_gz_url="https://www.apache.org/dyn/closer.lua/arrow/arrow-${version}/apache-arrow-${version}.tar.gz?action=download"
sha256sum=$(curl \
              --location \
              "https://www.apache.org/dyn/closer.lua?action=download&filename=arrow/arrow-${version}/apache-arrow-${version}.tar.gz.sha256" | \
              cut -d' ' -f1)
sed \
  -i.bak \
  -e "1a\ \ \"${version}\":" \
  -e "1a\ \ \ \ folder:\ all" \
  ${recipes_arrow}/config.yml
rm ${recipes_arrow}/config.yml.bak
sed \
  -i.bak \
  -e "1a\ \ \"${version}\":" \
  -e "1a\ \ \ \ url: \"${tar_gz_url}\"" \
  -e "1a\ \ \ \ sha256: \"${sha256sum}\"" \
  ${recipes_arrow}/all/conandata.yml
rm ${recipes_arrow}/all/conandata.yml.bak
git add ${recipes_arrow}/config.yml
git add ${recipes_arrow}/all/conandata.yml
git commit -m "arrow: add version ${version}"

git push origin ${branch}


owner=$(git remote get-url origin | \
          grep -o '[a-zA-Z0-9_-]*/conan-center-index' | \
          cut -d/ -f1)
echo "Create a pull request:"
echo "  https://github.com/${owner}/conan-center-index/pull/new/${branch}"
