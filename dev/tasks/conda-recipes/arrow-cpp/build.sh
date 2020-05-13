#!/bin/bash

set -e
set -x

mkdir cpp/build
pushd cpp/build

EXTRA_CMAKE_ARGS=""

# Include g++'s system headers
if [ "$(uname)" == "Linux" ]; then
  SYSTEM_INCLUDES=$(echo | ${CXX} -E -Wp,-v -xc++ - 2>&1 | grep '^ ' | awk '{print "-isystem;" substr($1, 1)}' | tr '\n' ';')
  EXTRA_CMAKE_ARGS=" -DARROW_GANDIVA_PC_CXX_FLAGS=${SYSTEM_INCLUDES}"
fi

cmake \
    -DARROW_BOOST_USE_SHARED=ON \
    -DARROW_BUILD_BENCHMARKS=OFF \
    -DARROW_BUILD_STATIC=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_UTILITIES=OFF \
    -DARROW_DATASET=ON \
    -DARROW_DEPENDENCY_SOURCE=SYSTEM \
    -DARROW_FLIGHT=ON \
    -DARROW_GANDIVA=ON \
    -DARROW_HDFS=ON \
    -DARROW_JEMALLOC=ON \
    -DARROW_MIMALLOC=ON \
    -DARROW_ORC=ON \
    -DARROW_PACKAGE_PREFIX=$PREFIX \
    -DARROW_PARQUET=ON \
    -DARROW_PLASMA=ON \
    -DARROW_PYTHON=ON \
    -DARROW_S3=ON \
    -DARROW_SIMD_LEVEL=NONE \
    -DARROW_WITH_BROTLI=ON \
    -DARROW_WITH_BZ2=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    -DCMAKE_AR=${AR} \
    -DCMAKE_BUILD_TYPE=release \
    -DCMAKE_INSTALL_LIBDIR=$PREFIX/lib \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_RANLIB=${RANLIB} \
    -DLLVM_TOOLS_BINARY_DIR=$PREFIX/bin \
    -GNinja \
    ${EXTRA_CMAKE_ARGS} \
    ..
ninja install

popd
