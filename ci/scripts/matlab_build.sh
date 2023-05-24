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

# Exit on error (-e) and print all commands (-x).
set -ex

base_dir=${1}
source_dir=${base_dir}/matlab
build_dir=${base_dir}/matlab/build
install_dir=${base_dir}/matlab/install

cmake \
  -S ${source_dir} \
  -B ${build_dir} \
  -G Ninja \
  -D MATLAB_ARROW_INTERFACE=ON \
  -D MATLAB_BUILD_TESTS=ON \
  -D CMAKE_INSTALL_PREFIX=${install_dir} \
  -D MATLAB_ADD_INSTALL_DIR_TO_SEARCH_PATH=OFF
cmake --build ${build_dir} --config Release --target install
ctest --test-dir ${build_dir}
