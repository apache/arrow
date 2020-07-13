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

export AWS_SDK_VERSION="1.7.356"
export CFLAGS="-fPIC"
export PREFIX="/usr/local"

curl -sL "https://github.com/aws/aws-sdk-cpp/archive/${AWS_SDK_VERSION}.tar.gz" -o aws-sdk-cpp-${AWS_SDK_VERSION}.tar.gz
tar xf aws-sdk-cpp-${AWS_SDK_VERSION}.tar.gz
pushd aws-sdk-cpp-${AWS_SDK_VERSION}

mkdir build
pushd build

cmake .. -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=${PREFIX} \
    -DBUILD_ONLY='s3;core;transfer;config' \
    -DBUILD_SHARED_LIBS=OFF \
    -DENABLE_CURL_LOGGING=ON \
    -DENABLE_UNITY_BUILD=ON \
    -DENABLE_TESTING=OFF

ninja install

popd
popd

rm -r aws-sdk-cpp-${AWS_SDK_VERSION}.tar.gz aws-sdk-cpp-${AWS_SDK_VERSION}
