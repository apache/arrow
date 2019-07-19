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

DC_VERSION=3.1.4

curl -sL https://github.com/google/double-conversion/archive/v${DC_VERSION}.tar.gz -o double-conversion-${DC_VERSION}.tar.gz
tar xf double-conversion-${DC_VERSION}.tar.gz
pushd double-conversion-${DC_VERSION}

cmake . -DCMAKE_INSTALL_PREFIX=/usr -DBUILD_SHARED_LIBS=OFF -DBUILD_TESTING=OFF -GNinja -DCMAKE_POSITION_INDEPENDENT_CODE=ON
ninja install

popd
rm -rf double-conversion-${DC_VERSION}.tar.gz double-conversion-${DC_VERSION}
