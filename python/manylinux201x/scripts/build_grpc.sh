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

export GRPC_VERSION="1.20.0"
export CFLAGS="-fPIC -DGPR_MANYLINUX1=1"
export PREFIX="/usr/local"

curl -sL "https://github.com/grpc/grpc/archive/v${GRPC_VERSION}.tar.gz" -o grpc-${GRPC_VERSION}.tar.gz
tar xf grpc-${GRPC_VERSION}.tar.gz
pushd grpc-${GRPC_VERSION}

cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=${PREFIX} \
      -DBUILD_SHARED_LIBS=OFF \
      -DCMAKE_C_FLAGS="${CFLAGS}" \
      -DCMAKE_CXX_FLAGS="${CFLAGS}" \
      -DgRPC_CARES_PROVIDER=package \
      -DgRPC_GFLAGS_PROVIDER=package \
      -DgRPC_PROTOBUF_PROVIDER=package \
      -DgRPC_SSL_PROVIDER=package \
      -DgRPC_ZLIB_PROVIDER=package \
      -DOPENSSL_USE_STATIC_LIBS=ON \
      -GNinja .
ninja install
popd
rm -rf grpc-${GRPC_VERSION}.tar.gz grpc-${GRPC_VERSION}
