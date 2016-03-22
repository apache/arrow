#!/usr/bin/env bash

set -e

: ${CPP_BUILD_DIR=$TRAVIS_BUILD_DIR/cpp-build}

pushd $CPP_BUILD_DIR

make lint

if [ $TRAVIS_OS_NAME == "linux" ]; then
  valgrind --tool=memcheck --leak-check=yes --error-exitcode=1 ctest -L unittest
else
  ctest -L unittest
fi

popd
