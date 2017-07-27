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

export SNAPPY_VERSION="1.1.3"
wget "https://github.com/google/snappy/releases/download/${SNAPPY_VERSION}/snappy-${SNAPPY_VERSION}.tar.gz" -O snappy-${SNAPPY_VERSION}.tar.gz
tar xf snappy-${SNAPPY_VERSION}.tar.gz
pushd snappy-${SNAPPY_VERSION}
./configure --with-pic "--prefix=/usr" CXXFLAGS='-DNDEBUG -O2'
make -j5
make install
popd
rm -rf snappy-${SNAPPY_VERSION}.tar.gz snappy-${SNAPPY_VERSION}
