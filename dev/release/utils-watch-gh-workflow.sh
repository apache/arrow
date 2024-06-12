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
set -o pipefail

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <tag> <workflow>"
    exit 1
fi

TAG=$1
WORKFLOW=$2
REPOSITORY="apache/arrow"

echo "Looking for GitHub Actions workflow on ${REPOSITORY}:${TAG}"
RUN_ID=""
while [[ -z "${RUN_ID}" ]]
do
    echo "Waiting for run to start..."
    RUN_ID=$(gh run list \
                --repo "${REPOSITORY}" \
                --workflow="${WORKFLOW}" \
                --json 'databaseId,event,headBranch,status' \
                --jq ".[] | select(.event == \"push\" and .headBranch == \"${TAG}\") | .databaseId")
      sleep 1
  done

echo "Found GitHub Actions workflow with ID: ${RUN_ID}"
gh run watch --repo "${REPOSITORY}" --exit-status ${RUN_ID}