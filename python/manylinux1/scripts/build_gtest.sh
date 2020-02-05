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

GTEST_VERSION=1.8.1

curl -sL https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz -o googletest-release-${GTEST_VERSION}.tar.gz
tar xf googletest-release-${GTEST_VERSION}.tar.gz
ls -l
pushd googletest-release-${GTEST_VERSION}

mkdir build_so
pushd build_so
cmake -DCMAKE_CXX_FLAGS='-fPIC' -Dgtest_force_shared_crt=ON -DBUILD_SHARED_LIBS=ON -DBUILD_GMOCK=ON -GNinja -DCMAKE_INSTALL_PREFIX=/usr ..
ninja install
popd

mkdir build_a
pushd build_a
cmake -DCMAKE_CXX_FLAGS='-fPIC' -Dgtest_force_shared_crt=ON -DBUILD_SHARED_LIBS=OFF -DBUILD_GMOCK=ON -GNinja -DCMAKE_INSTALL_PREFIX=/usr ..
ninja install
popd

popd
rm -rf googletest-release-${GTEST_VERSION}.tar.gz googletest-release-${GTEST_VERSION}
