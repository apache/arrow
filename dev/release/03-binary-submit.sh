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

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

version=$1
rc=$2
version_with_rc="${version}-rc${rc}"
crossbow_job_prefix="release-${version_with_rc}"
release_tag="apache-arrow-${version}"
rc_branch="release-${version_with_rc}"

: ${ARROW_REPOSITORY:="apache/arrow"}
: ${ARROW_BRANCH:=$release_tag}

# archery will submit a job with id: "${crossbow_job_prefix}-0" unless there
# are jobs submitted with the same prefix (the integer at the end is auto
# incremented)
archery crossbow submit \
    --no-fetch \
    --job-prefix ${crossbow_job_prefix} \
    --arrow-version ${version_with_rc} \
    --arrow-remote "https://github.com/${ARROW_REPOSITORY}" \
    --arrow-branch ${ARROW_BRANCH} \
    --group packaging

# archery will add a comment to the automatically generated PR to track
# the submitted jobs
job_name=$(archery crossbow latest-prefix --no-fetch ${crossbow_job_prefix})
archery crossbow report-pr \
    --no-fetch \
    --arrow-remote "https://github.com/${ARROW_REPOSITORY}" \
    --job-name ${job_name} \
    --pr-title "WIP: [Release] Verify ${rc_branch}"
