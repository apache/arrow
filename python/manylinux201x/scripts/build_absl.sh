#!/bin/bash -ex
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

export ABSL_VERSION="2eba343b51e0923cd3fb919a6abd6120590fc059"
export CFLAGS="-fPIC"
export CXXFLAGS="-fPIC"

curl -sL "https://github.com/abseil/abseil-cpp/archive/${ABSL_VERSION}.tar.gz" -o ${ABSL_VERSION}.tar.gz
tar xf ${ABSL_VERSION}.tar.gz
pushd abseil-cpp-${ABSL_VERSION}

cmake . -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr \
    -DABSL_RUN_TESTS=OFF \
    -DCMAKE_CXX_STANDARD=11

ninja install
popd
rm -rf abseil-cpp-${ABSL_VERSION} ${ABSL_VERSION}.tar.gz
