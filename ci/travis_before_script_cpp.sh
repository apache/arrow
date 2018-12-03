#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -ex

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

only_library_mode=no
using_homebrew=no

while true; do
    case "$1" in
	--only-library)
	    only_library_mode=yes
	    shift ;;
	--homebrew)
	    using_homebrew=yes
	    shift ;;
	*) break ;;
    esac
done

if [ "$only_library_mode" == "no" ]; then
  source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh
fi

CMAKE_COMMON_FLAGS="\
-DCMAKE_INSTALL_PREFIX=$ARROW_CPP_INSTALL \
-DARROW_NO_DEPRECATED_API=ON \
-DARROW_EXTRA_ERROR_CONTEXT=ON"
CMAKE_LINUX_FLAGS=""
CMAKE_OSX_FLAGS=""

if [ "$ARROW_TRAVIS_USE_TOOLCHAIN" == "1" ]; then
  # Set up C++ toolchain from conda-forge packages for faster builds
  source $TRAVIS_BUILD_DIR/ci/travis_install_toolchain.sh
  CMAKE_COMMON_FLAGS="${CMAKE_COMMON_FLAGS} -DARROW_JEMALLOC=ON"
  CMAKE_COMMON_FLAGS="${CMAKE_COMMON_FLAGS} -DARROW_WITH_BZ2=ON"
fi

mkdir -p $ARROW_CPP_BUILD_DIR
pushd $ARROW_CPP_BUILD_DIR

if [ $only_library_mode == "yes" ]; then
  CMAKE_COMMON_FLAGS="\
$CMAKE_COMMON_FLAGS \
-DARROW_BUILD_UTILITIES=OFF \
-DARROW_INSTALL_NAME_RPATH=OFF"
else
  CMAKE_COMMON_FLAGS="\
$CMAKE_COMMON_FLAGS \
-DARROW_BUILD_BENCHMARKS=ON \
-DARROW_BUILD_TESTS=ON \
-DARROW_BUILD_EXAMPLES=ON \
-DARROW_BUILD_UTILITIES=OFF \
-DARROW_INSTALL_NAME_RPATH=OFF"
fi

# Use Ninja for faster builds when using toolchain
if [ $ARROW_TRAVIS_USE_TOOLCHAIN == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -GNinja"
fi

if [ $ARROW_TRAVIS_PLASMA == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_PLASMA=ON"
fi

if [ $ARROW_TRAVIS_PLASMA_JAVA_CLIENT == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_PLASMA_JAVA_CLIENT=ON"
fi

if [ $ARROW_TRAVIS_ORC == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_ORC=ON"
fi

if [ $ARROW_TRAVIS_PARQUET == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS \
-DARROW_PARQUET=ON \
-DPARQUET_BUILD_EXAMPLES=ON \
-DPARQUET_BUILD_EXECUTABLES=ON"
fi

if [ $ARROW_TRAVIS_GANDIVA == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_GANDIVA=ON"
  if [ $only_library_mode == "no" ]; then
    CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_GANDIVA_BUILD_TESTS=ON"
  fi
fi

if [ $ARROW_TRAVIS_VALGRIND == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_TEST_MEMCHECK=ON"
fi

if [ $ARROW_TRAVIS_COVERAGE == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_GENERATE_COVERAGE=ON"
fi

if [ $ARROW_TRAVIS_VERBOSE == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_VERBOSE_THIRDPARTY_BUILD=ON"
fi

if [ $ARROW_TRAVIS_USE_VENDORED_BOOST == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_BOOST_VENDORED=ON"
fi

if [ $TRAVIS_OS_NAME == "linux" ]; then
    cmake $CMAKE_COMMON_FLAGS \
          $CMAKE_LINUX_FLAGS \
          -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
          -DBUILD_WARNING_LEVEL=$ARROW_BUILD_WARNING_LEVEL \
          $ARROW_CPP_DIR
else
    if [ "$using_homebrew" = "yes" ]; then
	# build against homebrew's boost if we're using it
	export BOOST_ROOT=$(brew --prefix boost)
	export LLVM_DIR=$(brew --prefix llvm@6)/lib/cmake/llvm
	export THRIFT_HOME=$(brew --prefix thrift)
    fi
    cmake $CMAKE_COMMON_FLAGS \
          $CMAKE_OSX_FLAGS \
          -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
          -DBUILD_WARNING_LEVEL=$ARROW_BUILD_WARNING_LEVEL \
          $ARROW_CPP_DIR
fi

# Build and install libraries
$TRAVIS_MAKE -j4
$TRAVIS_MAKE install

popd
