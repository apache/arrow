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

set -ex

source_dir=${1}/js
shard=${2}

pushd ${source_dir}

if [ -n "${shard}" ]; then
  # Run only when `1/n`.
  if [ "${shard:0:1}" = "1" ]; then
    yarn lint
    yarn test:bundle
  fi
  yarn test -- --shard=${shard}
else
  yarn lint
  yarn test
  yarn test:bundle
fi

popd
