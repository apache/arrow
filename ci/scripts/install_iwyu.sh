#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -eu

source_dir=${1:-/tmp/iwyu}
install_prefix=${2:-/usr/local}
clang_tools_version=${3:-8}

iwyu_branch_name="clang_${clang_tools_version}"
if [ ${clang_tools_version} -lt 10 ]; then
  iwyu_branch_name="${iwyu_branch_name}.0"
fi

git clone --single-branch --branch ${iwyu_branch_name} \
    https://github.com/include-what-you-use/include-what-you-use.git ${source_dir}

mkdir -p ${source_dir}/build
pushd ${source_dir}/build

# Build IWYU for current Clang
export CC=clang-${clang_tools_version}
export CXX=clang++-${clang_tools_version}

cmake -DCMAKE_PREFIX_PATH=/usr/lib/llvm-${clang_tools_version} \
      -DCMAKE_INSTALL_PREFIX=${install_prefix} \
      ${source_dir}
make -j4
make install

popd

rm -rf ${source_dir}
