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

export BZ2_VERSION="1.0.6"
export CFLAGS="-Wall -Winline -O2 -fPIC -D_FILE_OFFSET_BITS=64"

curl -sL "https://www.sourceware.org/pub/bzip2/bzip2-${BZ2_VERSION}.tar.gz" -o bzip2-${BZ2_VERSION}.tar.gz
tar xf bzip2-${BZ2_VERSION}.tar.gz

pushd bzip2-${BZ2_VERSION}
make PREFIX=/usr CFLAGS="$CFLAGS" install -j8
popd

rm -rf bzip2-${BZ2_VERSION}.tar.gz bzip2-${BZ2_VERSION}
