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

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <turbodbc version>"
  exit 1
fi

turbodbc=$1

source activate testenv

git clone --recurse-submodules https://github.com/blue-yonder/turbodbc /turbodbc
if [ "${turbodbc}" = "master" ]; then
  git -C /turbodbc checkout master;
elif [ "${turbodbc}" = "latest" ]; then
  git -C /turbodbc checkout $(git describe --tags);
else
  git -C /turbodbc checkout ${turbodbc};
fi

conda clean --all
