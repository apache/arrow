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

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

version=$1
rc=$2

case ${version} in
  *.0.0) # Major release
    echo "Do nothing for major release"
    ;;
  *.*.0) # Minor release
    echo "Minor release isn't supported yet"
    exit 1
    ;;
  *) # Patch release
    echo "Fetching all commits"
    git fetch --all --prune --tags --force -j$(nproc)

    release_branch=release-${version}-rc${rc}
    maint_branch=maint-$(echo ${version} | sed -e 's/\.[0-9]*$/.x/')
    echo "Merging ${release_branch} to ${merge_branch}"
    git branch -D ${maint_branch}
    git checkout -b ${maint_branch} apache/${maint_branch}
    git merge release-${version}-rc${rc}
    git push apache ${maint_branch}
    git checkout -
    ;;
  *)
esac
