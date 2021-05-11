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


ORC_VERSION="1.6.7"

curl -sL https://github.com/apache/orc/archive/rel/release-${ORC_VERSION}.tar.gz -o orc-${ORC_VERSION}.tar.gz
tar xf orc-${ORC_VERSION}.tar.gz 
pushd orc-rel-release-${ORC_VERSION}

mkdir build
pushd build

cmake \
    -DCMAKE_BUILD_TYPE=RELEASE \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DSTOP_BUILD_ON_WARNING=OFF \
    -DBUILD_LIBHDFSPP=OFF \
    -DZSTD_STATIC_LIB_NAME=zstd \
    -DBUILD_JAVA=OFF \
    -DBUILD_TOOLS=OFF \
    -DBUILD_CPP_TESTS=OFF \
    -DINSTALL_VENDORED_LIBS=OFF \
    -GNinja \
    ..
ninja install

popd # exit the build dir
popd # exit orc dir

rm -rf orc-rel-release-${ORC_VERSION} orc-${ORC_VERSION}.tar.gz