#!/bin/bash
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

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <version>"
  exit
fi

version=$1

base_url=https://dist.apache.org/repos/dist/dev/arrow
pattern="^apache-arrow-${version}-rc"
paths=$()
if svn ls ${base_url} | grep "${pattern}" > /dev/null 2>&1; then
  rc_paths=$(svn ls ${base_url} | grep "${pattern}")
  rc_urls=()
  for rc_path in ${rc_paths}; do
    rc_urls+=(${base_url}/${rc_path})
  done
  svn rm --message "Remove RC for ${version}" ${rc_urls[@]}
  echo "Removed RC artifacts:"
  for rc_url in ${rc_urls[@]}; do
    echo "  ${rc_url}"
  done
else
  echo "No RC artifacts at ${base_url}"
fi
