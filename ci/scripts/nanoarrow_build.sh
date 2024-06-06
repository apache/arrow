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

arrow_dir=${1}
source_dir=${1}/nanoarrow
build_dir=${2}/nanoarrow

# This file is used to build the nanoarrow binaries needed for the archery
# integration tests. Testing of the nanoarrow implementation in normal CI is handled
# by github workflows in the arrow-nanoarrow repository.

# Include in the integration test by default if the source directory is present
if [ -z  "${ARCHERY_INTEGRATION_WITH_NANOARROW}" ]; then
  ARCHERY_INTEGRATION_WITH_NANOARROW="1"
fi

if [ "${ARCHERY_INTEGRATION_WITH_NANOARROW}" -eq "0" ]; then
  echo "====================================================================="
  echo "Not building nanoarrow because ARCHERY_INTEGRATION_WITH_NANOARROW=0"
  echo "====================================================================="
  exit 0;
elif [ ! -d "${source_dir}" ]; then
  echo "====================================================================="
  echo "Not building nanoarrow because '${source_dir}' does not exist"
  echo "====================================================================="
  exit 0;
fi

set -x

mkdir -p ${build_dir}
pushd ${build_dir}

cmake ${source_dir} -DNANOARROW_BUILD_INTEGRATION_TESTS=ON
cmake --build .

popd
