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
CCACHE_VERSION=3.6

curl -sLO https://www.samba.org/ftp/ccache/ccache-${CCACHE_VERSION}.tar.bz2
tar xf ccache-${CCACHE_VERSION}.tar.bz2
pushd ccache-${CCACHE_VERSION}
./configure --prefix=/usr
make -j$NCORES
make install
popd
rm -rf ccache-${CCACHE_VERSION}.tar.bz2 ccache-${CCACHE_VERSION}

# Initialize the config directory, otherwise the build sometimes fails.
mkdir /root/.ccache
