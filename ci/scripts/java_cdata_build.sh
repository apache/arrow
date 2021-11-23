#!/usr/bin/env bash

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

arrow_dir=${1}
build_dir=${2}
# The directory where the final binaries will be stored when scripts finish
dist_dir=${3}

echo "=== Clear output directories and leftovers ==="
# Clear output directories and leftovers
rm -rf ${build_dir}

echo "=== Building Arrow Java C Data Interface native library ==="
mkdir -p "${build_dir}"
pushd "${build_dir}"

cmake \
  -DCMAKE_BUILD_TYPE=${ARROW_BUILD_TYPE:-release} \
  -DCMAKE_INSTALL_LIBDIR=lib \
  -DCMAKE_INSTALL_PREFIX=${build_dir} \
  ${arrow_dir}/java/c
cmake --build . --target install --config ${ARROW_BUILD_TYPE:-release}
popd

echo "=== Copying libraries to the distribution folder ==="
mkdir -p "${dist_dir}"
cp -L ${build_dir}/lib/*arrow_cdata_jni.* ${dist_dir}
