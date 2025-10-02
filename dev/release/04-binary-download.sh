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

set -e

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <version> <rc-num> [options]"
  exit
fi

version=$1
shift
rc_number=$1
shift
version_with_rc="${version}-rc${rc_number}"
crossbow_job_prefix="release-${version_with_rc}"
release_tag="apache-arrow-${version_with_rc}"

# archery will submit a job with id: "${crossbow_job_prefix}-0" unless there
# are jobs submitted with the same prefix (the integer at the end is auto
# incremented)
: ${CROSSBOW_JOB_NUMBER:="0"}
: ${CROSSBOW_JOB_ID:="${crossbow_job_prefix}-${CROSSBOW_JOB_NUMBER}"}

archery crossbow download-artifacts --no-fetch ${CROSSBOW_JOB_ID} "$@"

# Wait for the GitHub Workflow that creates the Linux packages
# to finish before downloading the artifacts.
. "${SOURCE_DIR}/utils-watch-gh-workflow.sh" "${release_tag}" "linux_packaging.yml"

RUN_ID=$(get_run_id)
# Download the artifacts created by the linux_packaging.yml workflow
download_artifacts "${SOURCE_DIR}/../../packages/${CROSSBOW_JOB_ID}"
