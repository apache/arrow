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

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <kartothek version> <target directory>"
  exit 1
fi

karthothek=$1
target=$2

git clone --recurse-submodules https://github.com/JDASoftwareGroup/kartothek "${target}"
if [ "${kartothek}" = "master" ]; then
  git -C "${target}" checkout master;
elif [ "${kartothek}" = "latest" ]; then
  git -C "${target}" checkout $(git describe --tags);
else
  git -C "${target}" checkout ${kartothek};
fi

pushd "${target}"
pip install --no-deps .
popd
