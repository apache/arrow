#!/bin/bash

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

set -e

set -x

# Builds arrow + gandiva and tests the same.
pushd cpp
  mkdir build
  pushd build
    CMAKE_FLAGS="-DCMAKE_BUILD_TYPE=Release \
          -DARROW_GANDIVA=ON \
          -DARROW_GANDIVA_JAVA=ON \
          -DARROW_GANDIVA_STATIC_LIBSTDCPP=ON \
          -DARROW_BUILD_TESTS=ON \
          -DARROW_BUILD_UTILITIES=OFF \
          -DARROW_BOOST_USE_SHARED=OFF"

    if [ $TRAVIS_OS_NAME == "osx" ]; then
      CMAKE_FLAGS="$CMAKE_FLAGS -DARROW_GFLAGS_USE_SHARED=OFF"
    fi

    cmake $CMAKE_FLAGS ..
    make -j4
    ctest

    cp -L release/libgandiva_jni.dylib $TRAVIS_BUILD_DIR/dist
  popd
popd
