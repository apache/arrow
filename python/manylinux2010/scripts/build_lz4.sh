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
export LZ4_VERSION="1.8.3"
export PREFIX="/usr/local"
export CFLAGS="${CFLAGS} -O3 -fPIC"
export LDFLAGS="${LDFLAGS} -Wl,-rpath,${PREFIX}/lib -L${PREFIX}/lib"
curl -sL "https://github.com/lz4/lz4/archive/v${LZ4_VERSION}.tar.gz" -o lz4-${LZ4_VERSION}.tar.gz
tar xf lz4-${LZ4_VERSION}.tar.gz
pushd lz4-${LZ4_VERSION}

make -j$NCORES PREFIX=${PREFIX}
make install PREFIX=${PREFIX}
popd
rm -rf lz4-${LZ4_VERSION}.tar.gz lz4-${LZ4_VERSION}
# We don't want to link against shared libs
rm -rf /usr/lib/liblz4.so*
