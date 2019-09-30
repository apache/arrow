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

# Set $ARROW_ROOT to the path of your Arrow clone and run

# docker build -t arrow_cpp_minimal .
# docker run --rm -t -i -v $PWD:/io -v $ARROW_ROOT:/arrow  arrow_cpp_minimal /io/build.sh

BUILD_DIR=/build
NPROC=$(nproc)

mkdir $BUILD_DIR
pushd $BUILD_DIR

cmake /arrow/cpp -DBOOST_SOURCE=BUNDLED \
      -DARROW_BOOST_USE_SHARED=OFF \
      -DARROW_COMPUTE=OFF \
      -DARROW_DATASET=OFF \
      -DARROW_FILESYSTEM=OFF \
      -DARROW_HDFS=OFF \
      -DARROW_JEMALLOC=OFF \
      -DARROW_JSON=OFF \
      -DARROW_USE_GLOG=OFF \
      -DARROW_WITH_BZ2=OFF \
      -DARROW_WITH_ZLIB=OFF \
      -DARROW_WITH_ZSTD=OFF \
      -DARROW_WITH_LZ4=OFF \
      -DARROW_WITH_SNAPPY=OFF \
      -DARROW_WITH_BROTLI=OFF \
      -DARROW_BUILD_UTILITIES=OFF

make -j$NPROC
make install

popd
