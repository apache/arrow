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

export ZSTD_VERSION="1.2.0"
export CFLAGS="${CFLAGS} -O3 -fPIC"
export PREFIX="/usr"
export LDFLAGS="${LDFLAGS} -Wl,-rpath,${PREFIX}/lib"
wget "https://github.com/facebook/zstd/archive/v${ZSTD_VERSION}.tar.gz" -O zstd-${ZSTD_VERSION}.tar.gz
tar xf zstd-${ZSTD_VERSION}.tar.gz
pushd zstd-${ZSTD_VERSION}

make -j5
make install PREFIX=$PREFIX
popd
rm -rf zstd-${ZSTD_VERSION}.tar.gz zstd-${ZSTD_VERSION}
