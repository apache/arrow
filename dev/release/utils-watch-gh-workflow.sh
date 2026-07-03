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

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "Usage: $0 <tag> <workflow> [run-id]"
  exit 1
fi

TAG=$1
WORKFLOW=$2
RUN_ID="${3:-}"
: "${REPOSITORY:=${GITHUB_REPOSITORY:-apache/arrow}}"

if [ -z "${RUN_ID}" ]; then
  echo "Looking for GitHub Actions workflow on ${REPOSITORY}:${TAG} (any ID)"
else
  echo "Looking for GitHub Actions workflow on ${REPOSITORY}:${TAG} with run ID ${RUN_ID}"
fi
if [ -z "${RUN_ID}" ]; then
  while true; do
    echo "Waiting for run to start..."
    RUN_ID=$(gh run list \
                --branch "${TAG}" \
                --jq '.[].databaseId' \
                --json databaseId \
                --limit 1 \
                --repo "${REPOSITORY}" \
                --status "in_progress" \
                --workflow "${WORKFLOW}")
    if [ -n "${RUN_ID}" ]; then
      break
    fi
    sleep 60
  done
  echo "Found GitHub Actions workflow with ID: ${RUN_ID}"
else
  echo "Using provided run ID: ${RUN_ID}. Sleeping for 10 seconds to let the job become available..."
  sleep 10
fi

gh run watch \
   --exit-status \
   --interval 60 \
   --repo "${REPOSITORY}" \
   ${RUN_ID}
