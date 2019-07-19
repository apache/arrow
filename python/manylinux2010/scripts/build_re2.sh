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

export RE2_VERSION="2019-04-01"
NCORES=$(($(grep -c ^processor /proc/cpuinfo) + 1))

curl -sL "http://github.com/google/re2/archive/${RE2_VERSION}.tar.gz" -o re2-${RE2_VERSION}.tar.gz
tar xf re2-${RE2_VERSION}.tar.gz
pushd re2-${RE2_VERSION}

export CXXFLAGS="-fPIC -O2 ${CXXFLAGS}"
export CFLAGS="-fPIC -O2 ${CFLAGS}"

# Build shared libraries
make prefix=/usr/local -j${NCORES} install

popd

# Need to remove shared library to make sure the static library is picked up by Arrow
rm -rf re2-${RE2_VERSION}.tar.gz re2-${RE2_VERSION} /usr/local/lib/libre2.so*
