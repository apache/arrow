#!/bin/bash

set -e
set -x

export PARQUET_BUILD_TOOLCHAIN=$PREFIX

mkdir build-dir
cd build-dir

cmake \
    -DCMAKE_BUILD_TYPE=release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_INSTALL_LIBDIR=$PREFIX/lib \
    -DPARQUET_BOOST_USE_SHARED=ON \
    -DPARQUET_BUILD_BENCHMARKS=OFF \
    -DPARQUET_BUILD_EXECUTABLES=OFF \
    -DPARQUET_BUILD_TESTS=OFF \
    ..

make -j${CPU_COUNT}
make install
