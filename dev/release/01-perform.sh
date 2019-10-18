#!/bin/bash
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
#
set -e

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

pushd "${SOURCE_DIR}/../../java"
git submodule update --init --recursive

profile=arrow-jni # this includes components which depend on arrow cpp.

cpp_dir="${PWD}/../cpp"
cpp_build_dir=$(mktemp -d -t "apache-arrow-cpp.XXXXX")
pushd ${cpp_build_dir}
cmake \
  -DARROW_GANDIVA=ON \
  -DARROW_GANDIVA_JAVA=ON \
  -DARROW_JNI=ON \
  -DARROW_ORC=ON \
  -DCMAKE_BUILD_TYPE=release \
  -G Ninja \
  "${cpp_dir}"
ninja
popd

export ARROW_TEST_DATA=${PWD}/../testing/data
mvn \
  release:perform \
  -Darguments=-Darrow.cpp.build.dir=${cpp_build_dir}/release \
  -P ${profile}
rm -rf ${cpp_build_dir}

popd
