#!/bin/bash

set -e
set -x

# Build dependencies
export ARROW_BUILD_TOOLCHAIN=$PREFIX

cd cpp
mkdir build-dir
cd build-dir

cmake \
    -DCMAKE_BUILD_TYPE=release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_INSTALL_LIBDIR=$PREFIX/lib \
    -DARROW_BOOST_USE_SHARED=ON \
    -DARROW_BUILD_BENCHMARKS=OFF \
    -DARROW_BUILD_UTILITIES=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_JEMALLOC=OFF \
    -DARROW_PLASMA=ON \
    -DARROW_PYTHON=ON \
    -DARROW_ORC=ON \
    ..

make -j${CPU_COUNT}
make install
