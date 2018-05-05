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

curl -sL https://github.com/google/googletest/archive/release-1.7.0.tar.gz -o googletest-release-1.7.0.tar.gz
tar xf googletest-release-1.7.0.tar.gz
ls -l
pushd googletest-release-1.7.0
cmake -DCMAKE_CXX_FLAGS='-fPIC' -Dgtest_force_shared_crt=ON .
make -j5
popd
rm -rf googletest-release-1.7.0.tar.gz
