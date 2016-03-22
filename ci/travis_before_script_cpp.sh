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

cmake -DARROW_BUILD_BENCHMARKS=ON -DCMAKE_INSTALL_PREFIX=$ARROW_CPP_INSTALL -DCMAKE_CXX_FLAGS="-Werror" $CPP_DIR
make -j4
make install

popd
