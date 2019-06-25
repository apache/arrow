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

export URIPARSER_VERSION="0.9.2"
export CFLAGS="-fPIC"
export PREFIX="/usr/local"
curl -sL "https://github.com/uriparser/uriparser/archive/uriparser-${URIPARSER_VERSION}.tar.gz" -o uriparser-${URIPARSER_VERSION}.tar.gz
tar xf uriparser-${URIPARSER_VERSION}.tar.gz
pushd uriparser-uriparser-${URIPARSER_VERSION}

cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=${PREFIX} \
      -DURIPARSER_BUILD_DOCS=off \
      -DURIPARSER_BUILD_TESTS=off \
      -DURIPARSER_BUILD_TOOLS=off \
      -DURIPARSER_BUILD_WCHAR_T=off \
      -DBUILD_SHARED_LIBS=off \
      -DCMAKE_POSITION_INDEPENDENT_CODE=on \
      -GNinja .
ninja install
popd
rm -rf uriparser-${URIPARSER_VERSION}.tar.gz uriparser-uriparser-${URIPARSER_VERSION}
