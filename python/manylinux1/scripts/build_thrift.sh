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

export THRIFT_VERSION=0.10.0
wget http://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz
tar xf thrift-${THRIFT_VERSION}.tar.gz
pushd thrift-${THRIFT_VERSION}
mkdir build-tmp
pushd build-tmp
cmake -DCMAKE_BUILD_TYPE=release \
    "-DCMAKE_CXX_FLAGS=-fPIC" \
    "-DCMAKE_C_FLAGS=-fPIC" \
    "-DCMAKE_INSTALL_PREFIX=/usr" \
    "-DCMAKE_INSTALL_RPATH=/usr/lib" \
    "-DBUILD_SHARED_LIBS=OFF" \
    "-DBUILD_TESTING=OFF" \
    "-DWITH_QT4=OFF" \
    "-DWITH_C_GLIB=OFF" \
    "-DWITH_JAVA=OFF" \
    "-DWITH_PYTHON=OFF" \
    "-DWITH_CPP=ON" \
    "-DWITH_STATIC_LIB=ON" ..
make -j5
make install
popd
popd
rm -rf thrift-${THRIFT_VERSION}.tar.gz thrift-${THRIFT_VERSION}
