#!/bin/bash

set -e
set -x

cd $RECIPE_DIR

# Build dependencies
export FLATBUFFERS_HOME=$PREFIX
export PARQUET_HOME=$PREFIX

cd ..

rm -rf conda-build
mkdir conda-build

cp -r thirdparty conda-build/

cd conda-build
pwd

# Build googletest for running unit tests
./thirdparty/download_thirdparty.sh
./thirdparty/build_thirdparty.sh gtest

source thirdparty/versions.sh
export GTEST_HOME=`pwd`/thirdparty/$GTEST_BASEDIR

if [ `uname` == Linux ]; then
    SHARED_LINKER_FLAGS='-static-libstdc++'
elif [ `uname` == Darwin ]; then
    SHARED_LINKER_FLAGS=''
fi

cmake \
    -DCMAKE_BUILD_TYPE=debug \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_SHARED_LINKER_FLAGS=$SHARED_LINKER_FLAGS \
    -DARROW_IPC=on \
    -DARROW_PARQUET=on \
    ..

make
ctest -L unittest
make install
