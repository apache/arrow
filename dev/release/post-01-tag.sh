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
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

# Create the release tag and trigger the Publish Release workflow.
release_candidate_tag=apache-arrow-${version}-rc${num}
release_tag=apache-arrow-${version}
git tag -a ${release_tag} ${release_candidate_tag}^{} -m "[Release] Apache Arrow Release ${version}"
git push origin ${release_tag}

# Wait for the Publish Release workflow to finish.
sleep 2s
workflow_id=$(gh run list --repo apache/arrow --workflow "Publish Release" | cut -f7 | head -n1)
gh run watch ${workflow_id} --repo apache/arrow
