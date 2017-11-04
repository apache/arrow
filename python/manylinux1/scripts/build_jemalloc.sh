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

wget https://github.com/jemalloc/jemalloc/archive/17c897976c60b0e6e4f4a365c751027244dada7a.tar.gz -O jemalloc.tar.gz
tar xf jemalloc.tar.gz
mv jemalloc-* jemalloc
pushd /jemalloc
./autogen.sh
./configure "--with-jemalloc-prefix=je_arrow_" "--with-private-namespace=je_arrow_private_"
# Skip doc generation
touch doc/jemalloc.html
touch doc/jemalloc.3
make -j5
make install
popd
rm -rf jemalloc.tar.gz jemalloc
