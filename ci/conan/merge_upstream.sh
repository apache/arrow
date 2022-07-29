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

set -eu

source_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 CONAN_CENTER_INDEX_WORK_DIRECTORY"
  echo " e.g.: $0 ~/work/conan/conan-center-index"
  exit 1
fi

conan_center_index="$1"

. "${source_dir}/merge_status.sh"

UPSTREAM_HEAD=$(git -C "${conan_center_index}" log -n1 --format=%H)
git \
  -C "${conan_center_index}" \
  diff \
  ${UPSTREAM_REVISION}..${UPSTREAM_HEAD} \
  recipes/arrow | \
  (cd "${source_dir}" && patch -p3)

sed \
  -i.bak \
  -E \
  -e "s/^(UPSTREAM_REVISION)=.*$/\\1=${UPSTREAM_HEAD}/g" \
  "${source_dir}/merge_status.sh"
rm "${source_dir}/merge_status.sh.bak"
