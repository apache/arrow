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

wget https://github.com/jemalloc/jemalloc/releases/download/4.4.0/jemalloc-4.4.0.tar.bz2 -O jemalloc-4.4.0.tar.bz2
tar xf jemalloc-4.4.0.tar.bz2
pushd /jemalloc-4.4.0
./configure "--with-jemalloc-prefix=je_arrow_" "--with-private-namespace=je_arrow_private_"
make -j5
make install
popd
rm -rf jemalloc-4.4.0.tar.bz2 jemalloc-4.4.0
