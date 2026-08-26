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
set -o pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

. "${SOURCE_DIR}/utils-env.sh"

version=$1
rc=$2

rc_tag="apache-arrow-${version}-rc${rc}"
repository="${REPOSITORY:-apache/arrow}"

run_id=$(gh run list \
            --branch "${rc_tag}" \
            --jq '.[].databaseId' \
            --json databaseId \
            --limit 1 \
            --repo "${repository}" \
            --workflow "verify_rc.yml")
gh run rerun \
   "${run_id}" \
   --failed \
   --repo "${repository}"
