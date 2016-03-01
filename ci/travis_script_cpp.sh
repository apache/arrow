#!/usr/bin/env bash

set -e

mkdir $TRAVIS_BUILD_DIR/cpp-build
pushd $TRAVIS_BUILD_DIR/cpp-build

CPP_DIR=$TRAVIS_BUILD_DIR/cpp

# Build an isolated thirdparty
cp -r $CPP_DIR/thirdparty .
cp $CPP_DIR/setup_build_env.sh .

if [ $TRAVIS_OS_NAME == "linux" ]; then
  # Use a C++11 compiler on Linux
  export CC="gcc-4.9"
  export CXX="g++-4.9"
fi

source setup_build_env.sh

echo $GTEST_HOME

cmake -DCMAKE_CXX_FLAGS="-Werror" $CPP_DIR
make lint
make -j4

if [ $TRAVIS_OS_NAME == "linux" ]; then
  valgrind --tool=memcheck --leak-check=yes --error-exitcode=1 ctest
else
  ctest
fi

popd
rm -rf cpp-build
