#!/usr/bin/env bash

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

# This script downloads all the thirdparty dependencies as a series of tarballs
# that can be used for offline builds, etc.

set -e

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <destination-directory>"
  exit
fi

_DST=$1

BOOST_VERSION=1.67.0
GTEST_VERSION=1.8.0
GFLAGS_VERSION=2.2.0
GBENCHMARK_VERSION=1.4.1
FLATBUFFERS_VERSION=1.9.0
RAPIDJSON_VERSION=1.1.0
JEMALLOC_VERSION=17c897976c60b0e6e4f4a365c751027244dada7a
SNAPPY_VERSION=1.1.3
BROTLI_VERSION=v0.6.0
LZ4_VERSION=1.7.5
ZLIB_VERSION=1.2.8
ZSTD_VERSION=1.2.0
PROTOBUF_VERSION=2.6.0
GRPC_VERSION=1.12.1
ORC_VERSION=1.5.1

BOOST_UNDERSCORE_VERSION=`echo $BOOST_VERSION | sed 's/\./_/g'`
wget -O $_DST/boost.tar.gz https://dl.bintray.com/boostorg/release/$BOOST_VERSION/source/boost_$BOOST_UNDERSCORE_VERSION.tar.gz

wget -O $_DST/gtest.tar.gz https://github.com/google/googletest/archive/release-$GTEST_VERSION.tar.gz

wget -O $_DST/gflags.tar.gz https://github.com/gflags/gflags/archive/v$GFLAGS_VERSION.tar.gz

wget -O $_DST/gbenchmark.tar.gz https://github.com/google/benchmark/archive/v$GBENCHMARK_VERSION.tar.gz

wget -O $_DST/flatbuffers.tar.gz https://github.com/google/flatbuffers/archive/v$FLATBUFFERS_VERSION.tar.gz

wget -O $_DST/rapidjson.tar.gz https://github.com/miloyip/rapidjson/archive/v$RAPIDJSON_VERSION.tar.gz

wget -O $_DST/snappy.tar.gz https://github.com/google/snappy/releases/download/$SNAPPY_VERSION/snappy-$SNAPPY_VERSION.tar.gz

wget -O $_DST/brotli.tar.gz https://github.com/google/brotli/archive/$BROTLI_VERSION.tar.gz

wget -O $_DST/lz4.tar.gz https://github.com/lz4/lz4/archive/v$LZ4_VERSION.tar.gz

wget -O $_DST/zlib.tar.gz http://zlib.net/fossils/zlib-$ZLIB_VERSION.tar.gz

wget -O $_DST/zstd.tar.gz https://github.com/facebook/zstd/archive/v$ZSTD_VERSION.tar.gz

wget -O $_DST/protobuf.tar.gz https://github.com/google/protobuf/releases/download/v$PROTOBUF_VERSION/protobuf-$PROTOBUF_VERSION.tar.gz

wget -O $_DST/grpc.tar.gz https://github.com/grpc/grpc/archive/v$GRPC_VERSION.tar.gz

wget -O $_DST/orc.tar.gz https://github.com/apache/orc/archive/rel/release-$ORC_VERSION.tar.gz

echo "
# Environment variables for offline Arrow build
export ARROW_BOOST_URL=$_DST/boost.tar.gz
export ARROW_GTEST_URL=$_DST/gtest.tar.gz
export ARROW_GFLAGS_URL=$_DST/gflags.tar.gz
export ARROW_GBENCHMARK_URL=$_DST/gbenchmark.tar.gz
export ARROW_FLATBUFFERS_URL=$_DST/flatbuffers.tar.gz
export ARROW_RAPIDJSON_URL=$_DST/rapidjson.tar.gz
export ARROW_SNAPPY_URL=$_DST/snappy.tar.gz
export ARROW_BROTLI_URL=$_DST/brotli.tar.gz
export ARROW_LZ4_URL=$_DST/lz4.tar.gz
export ARROW_ZLIB_URL=$_DST/zlib.tar.gz
export ARROW_ZSTD_URL=$_DST/zstd.tar.gz
export ARROW_PROTOBUF_URL=$_DST/protobuf.tar.gz
export ARROW_GRPC_URL=$_DST/grpc.tar.gz
export ARROW_ORC_URL=$_DST/orc.tar.gz
"
