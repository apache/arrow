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

export CARES_VERSION="1.15.0"
export CFLAGS="-fPIC"
export PREFIX="/usr"
curl -sL "https://c-ares.haxx.se/download/c-ares-$CARES_VERSION.tar.gz" -o c-ares-${CARES_VERSION}.tar.gz
tar xf c-ares-${CARES_VERSION}.tar.gz
pushd c-ares-${CARES_VERSION}

cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=${PREFIX} \
      -DCARES_STATIC=ON \
      -DCARES_SHARED=OFF \
      -DCMAKE_C_FLAGS=${CFLAGS} \
      -GNinja .
ninja install
popd
rm -rf c-ares-${CARES_VERSION}.tar.gz c-ares-${CARES_VERSION}
