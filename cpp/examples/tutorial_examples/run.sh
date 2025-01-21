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

set -e

cd /io

export ARROW_BUILD_DIR=/build/arrow
export EXAMPLE_BUILD_DIR=/build/example
export ARROW_INSTALL_DIR=/build/arrow-install

echo
echo "=="
echo "== Building Arrow C++ library"
echo "=="
echo

ARROW_CMAKE_OPTIONS="-DCMAKE_INSTALL_PREFIX=${ARROW_INSTALL_DIR} \
                     -DCMAKE_INSTALL_RPATH=${ARROW_INSTALL_DIR}/lib" \
  ./build_arrow.sh

echo
echo "=="
echo "== Building example project using Arrow C++ library"
echo "=="
echo

EXAMPLE_CMAKE_OPTIONS="-DCMAKE_INSTALL_RPATH=${ARROW_INSTALL_DIR}/lib \
                       -DCMAKE_PREFIX_PATH=${ARROW_INSTALL_DIR}" \
  ./build_example.sh

echo
echo "=="
echo "== Running example project"
echo "=="
echo

${EXAMPLE_BUILD_DIR}/arrow_example
${EXAMPLE_BUILD_DIR}/compute_example
${EXAMPLE_BUILD_DIR}/file_access_example
${EXAMPLE_BUILD_DIR}/dataset_example
