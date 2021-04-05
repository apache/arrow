#!/bin/bash

export MAKEFLAGS="-j$(grep -c ^processor /proc/cpuinfo)"
mkdir -p ../cpp/build && pushd ../cpp/build

# configure the build using ninja
# see https://arrow.apache.org/docs/developers/cpp/building.html#faster-builds-with-ninja
# no S3 AWS SDK support
cmake -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
 -DCMAKE_INSTALL_LIBDIR=lib \
 -DARROW_WITH_BZ2=ON \
 -DARROW_WITH_ZLIB=ON \
 -DARROW_WITH_ZSTD=ON \
 -DARROW_WITH_LZ4=$ARROW_WITH_LZ4 \
 -DARROW_WITH_SNAPPY=$ARROW_WITH_BROTLI \
 -DARROW_WITH_BROTLI=ON \
 -DARROW_PARQUET=ON \
 -DARROW_PYTHON=OFF \
 -DARROW_BUILD_TESTS=OFF \
 -DARROW_COMPUTE=ON \
 -DARROW_CSV=ON \
 -DARROW_DATASET=ON \
 -DARROW_FILESYSTEM=ON \
 -DARROW_JEMALLOC=ON \
 -DARROW_JSON=ON \
 -DCMAKE_BUILD_TYPE=release \
 -DARROW_INSTALL_NAME_RPATH=OFF \
 -DARROW_S3=ON \
 -GNinja \
 ..

cmake --build . --target install

pushd ../../r

