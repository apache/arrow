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

export GLOG_VERSION="0.4.0"
export PREFIX="/usr/local"
curl -sL "https://github.com/google/glog/archive/v${GLOG_VERSION}.tar.gz" -o glog-${GLOG_VERSION}.tar.gz
tar xf glog-${GLOG_VERSION}.tar.gz
pushd glog-${GLOG_VERSION}

cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=${PREFIX} \
      -DCMAKE_POSITION_INDEPENDENT_CODE=1 \
      -DBUILD_SHARED_LIBS=OFF \
      -DBUILD_TESTING=OFF \
      -DWITH_GFLAGS=OFF \
      -GNinja .
ninja install
popd
rm -rf glog-${GLOG_VERSION}.tar.gz glog-${GLOG_VERSION}

