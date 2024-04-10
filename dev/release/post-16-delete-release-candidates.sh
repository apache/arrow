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

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <version>"
  exit
fi

version=$1

repo=apache/arrow
tags=$(gh release list --repo ${repo} | cut -f3 | grep -F "apache-arrow-${version}-rc")
tags_with_spaces=$(echo ${tags})
for tag in ${tags_with_spaces}
do
  # TODO: Should we supply --yes to skip the confirmation prompt?
  gh release delete ${tag} --cleanup-tag --repo ${repo}
done

