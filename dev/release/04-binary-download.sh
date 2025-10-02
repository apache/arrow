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
tag="apache-arrow-${version_with_rc}"

# archery will submit a job with id: "${crossbow_job_prefix}-0" unless there
# are jobs submitted with the same prefix (the integer at the end is auto
# incremented)
: ${CROSSBOW_JOB_NUMBER:="0"}
: ${CROSSBOW_JOB_ID:="${crossbow_job_prefix}-${CROSSBOW_JOB_NUMBER}"}

archery crossbow download-artifacts --no-fetch ${CROSSBOW_JOB_ID} "$@"

# Download Linux packages.
gh release download "${tag}" \
  --dir "${crossbow_job_prefix}" \
  --pattern "almalinux-*.tar.gz" \
  --pattern "amazon-linux-*.tar.gz" \
  --pattern "centos-*.tar.gz" \
  --pattern "debian-*.tar.gz" \
  --pattern "ubuntu-*.tar.gz" \
  --repo "${REPOSITORY:-apache/arrow}" \
  --skip-existing
