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

export FLATBUFFERS_VERSION=1.10.0
curl -sL https://github.com/google/flatbuffers/archive/v${FLATBUFFERS_VERSION}.tar.gz \
    -o flatbuffers-${FLATBUFFERS_VERSION}.tar.gz
tar xf flatbuffers-${FLATBUFFERS_VERSION}.tar.gz
pushd flatbuffers-${FLATBUFFERS_VERSION}
cmake \
    "-DCMAKE_CXX_FLAGS=-fPIC" \
    "-DCMAKE_INSTALL_PREFIX:PATH=/usr" \
    -DFLATBUFFERS_BUILD_TESTS=OFF \
    -DCMAKE_BUILD_TYPE=Release \
    -GNinja
ninja install
popd
rm -rf flatbuffers-${FLATBUFFERS_VERSION}.tar.gz flatbuffers-${FLATBUFFERS_VERSION}
