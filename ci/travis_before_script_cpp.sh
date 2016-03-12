#!/usr/bin/env bash

set -e

: ${CPP_BUILD_DIR=$TRAVIS_BUILD_DIR/cpp-build}

mkdir $CPP_BUILD_DIR
pushd $CPP_BUILD_DIR

CPP_DIR=$TRAVIS_BUILD_DIR/cpp

# Build an isolated thirdparty
cp -r $CPP_DIR/thirdparty .
cp $CPP_DIR/setup_build_env.sh .

source setup_build_env.sh

echo $GTEST_HOME

: ${ARROW_CPP_INSTALL=$TRAVIS_BUILD_DIR/cpp-install}

CMAKE_COMMON_FLAGS="-DARROW_BUILD_BENCHMARKS=ON -DCMAKE_INSTALL_PREFIX=$ARROW_CPP_INSTALL"

if [ $TRAVIS_OS_NAME == "linux" ]; then
  cmake -DARROW_TEST_MEMCHECK=on $CMAKE_COMMON_FLAGS -DCMAKE_CXX_FLAGS="-Werror" $CPP_DIR
else
  cmake $CMAKE_COMMON_FLAGS -DCMAKE_CXX_FLAGS="-Werror" $CPP_DIR
fi

make -j4
make install

popd
