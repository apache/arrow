#!/usr/bin/env bash
#
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

set -e

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <prefix>"
  exit 1
fi

version=$1
prefix=$2

url="http://ftp.gnu.org/gnu/glibc/glibc-${version}.tar.gz"

mkdir /tmp/glibc
wget -q ${url} -O - | tar -xzf - --directory /tmp/glibc --strip-components=1

mkdir /tmp/glibc/build
pushd /tmp/glibc/build
../configure --prefix=${prefix}
make -j$(nproc)
make install
popd

rm -rf /tmp/glibc
