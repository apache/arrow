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

export GFLAGS_VERSION="2.2.1"
export CFLAGS="-fPIC"
export CXXFLAGS="-fPIC"

curl -sL "https://github.com/gflags/gflags/archive/v${GFLAGS_VERSION}.tar.gz" -o gflags-${GFLAGS_VERSION}.tar.gz
tar xf gflags-${GFLAGS_VERSION}.tar.gz
pushd gflags-${GFLAGS_VERSION}

cmake .  \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr \
    -DINSTALL_HEADERS=on \
    -DBUILD_SHARED_LIBS=off \
    -DBUILD_STATIC_LIBS=on \
    -DBUILD_TESTING=off \
    -GNinja

ninja install
popd
rm -rf gflags-${GFLAGS_VERSION}.tar.gz gflags-${GFLAGS_VERSION}
