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
set -o pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc-num>"
  exit
fi

version=$1
rc=$2

# Create the release tag and trigger the Publish Release workflow.
release_tag=apache-arrow-${version}
release_candidate_tag=${release_tag}-rc${rc}
git tag -a ${release_tag} ${release_candidate_tag} -m "[Release] Apache Arrow Release ${version}"
git push upstream ${release_tag}
