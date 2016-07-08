#!/usr/bin/env bash

set -e

: ${CPP_BUILD_DIR=$TRAVIS_BUILD_DIR/cpp-build}

pushd $CPP_BUILD_DIR

make lint

# ARROW-209: checks depending on the LLVM toolchain are disabled temporarily
# until we are able to install the full LLVM toolchain in Travis CI again

# if [ $TRAVIS_OS_NAME == "linux" ]; then
#   make check-format
#   make check-clang-tidy
# fi

ctest -VV -L unittest

popd
