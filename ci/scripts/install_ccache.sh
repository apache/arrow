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

mkdir -p /tmp/ccache
case $(uname) in
  MINGW64*)
    url="https://github.com/ccache/ccache/releases/download/v${version}/ccache-${version}-windows-x86_64.zip"
    pushd /tmp/ccache
    curl --fail --location --remote-name ${url}
    unzip -j ccache-${version}-windows-x86_64.zip
    chmod +x ccache.exe
    mv ccache.exe ${prefix}/bin/
    popd
    ;;
  *)
    url="https://github.com/ccache/ccache/archive/v${version}.tar.gz"

    wget -q ${url} -O - | tar -xzf - --directory /tmp/ccache --strip-components=1

    mkdir /tmp/ccache/build
    pushd /tmp/ccache/build
    cmake \
      -GNinja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=${prefix} \
      -DZSTD_FROM_INTERNET=ON \
      ..
    ninja install
    popd
    ;;
esac
rm -rf /tmp/ccache
