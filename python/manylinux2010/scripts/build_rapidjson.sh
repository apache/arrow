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

export RAPIDJSON_VERSION="1.1.0"

curl -sL "https://github.com/miloyip/rapidjson/archive/v${RAPIDJSON_VERSION}.tar.gz" -o rapidjson-${RAPIDJSON_VERSION}.tar.gz
tar xf rapidjson-${RAPIDJSON_VERSION}.tar.gz
pushd rapidjson-${RAPIDJSON_VERSION}
mkdir build
pushd build
cmake -GNinja \
      -DRAPIDJSON_HAS_STDSTRING=ON \
      -DCMAKE_INSTALL_PREFIX=/usr \
      -DRAPIDJSON_BUILD_TESTS=OFF \
      -DRAPIDJSON_BUILD_EXAMPLES=OFF \
      -DRAPIDJSON_BUILD_DOC=OFF \
      -DCMAKE_BUILD_TYPE=release \
      ..
ninja install
popd
popd
rm -rf rapidjson-${RAPIDJSON_VERSION}.tar.gz rapidjson-${RAPIDJSON_VERSION}
