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

NCORES=$(($(grep -c ^processor /proc/cpuinfo) + 1))
export UTF8PROC_VERSION="2.5.0"
export PREFIX="/usr/local"

curl -sL "https://github.com/JuliaStrings/utf8proc/archive/v${UTF8PROC_VERSION}.tar.gz" -o utf8proc-$UTF8PROC_VERSION}.tar.gz
tar xf utf8proc-$UTF8PROC_VERSION}.tar.gz

pushd utf8proc-${UTF8PROC_VERSION}
mkdir build
pushd build
cmake .. -GNinja \
  -DBUILD_SHARED_LIBS=OFF \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=${PREFIX}

ninja install
popd
popd

rm -rf utf8proc-${UTF8PROC_VERSION}.tar.gz utf8proc-${UTF8PROC_VERSION}
