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
  echo "Usage: $0 <version> <microsoft/vcpkg's fork repository>"
  exit 1
fi

version=$1
repository=$2

if [ ! -d "${repository}" ]; then
  echo "microsoft/vcpkg's fork repository doesn't exist: ${repository}"
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
  echo "  git remote add upstream https://github.com/microsoft/vcpkg.git"
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

port_arrow=ports/arrow
echo "Updating: ${port_arrow}"
sha512sum=$(curl \
              --location \
              "https://www.apache.org/dyn/closer.lua?action=download&filename=arrow/arrow-${version}/apache-arrow-${version}.tar.gz.sha512" | \
              cut -d' ' -f1)
sed \
  -i.bak \
  -e "s/^  \"version\": \".*\",$/  \"version\": \"${version}\",/" \
  ${port_arrow}/vcpkg.json
rm ${port_arrow}/vcpkg.json.bak
sed \
  -i.bak \
  -e "s/^    SHA512 .*$/    SHA512 ${sha512sum}/" \
  ${port_arrow}/portfile.cmake
rm ${port_arrow}/portfile.cmake.bak
git add ${port_arrow}/vcpkg.json
git add ${port_arrow}/portfile.cmake
git commit -m "[arrow] Update to ${version}"

./vcpkg x-add-version --overwrite-version arrow
git add versions
git commit -m "Update versions"

git push origin ${branch}


owner=$(git remote get-url origin | \
          grep -o '[a-zA-Z0-9_-]*/vcpkg' | \
          cut -d/ -f1)
echo "Create a pull request:"
echo "  https://github.com/${owner}/vcpkg/pull/new/${branch}"
echo
echo "  Title: [arrow] Update to ${version}"
