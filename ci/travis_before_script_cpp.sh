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

if [ "$ARROW_TRAVIS_USE_TOOLCHAIN" == "1" ]; then
  # Set up C++ toolchain from conda-forge packages for faster builds
  source $TRAVIS_BUILD_DIR/ci/travis_install_toolchain.sh
fi

mkdir -p $ARROW_CPP_BUILD_DIR
pushd $ARROW_CPP_BUILD_DIR

CMAKE_COMMON_FLAGS="\
-DCMAKE_INSTALL_PREFIX=$ARROW_CPP_INSTALL \
-DARROW_NO_DEPRECATED_API=ON \
-DARROW_INSTALL_NAME_RPATH=OFF \
-DARROW_EXTRA_ERROR_CONTEXT=ON"
CMAKE_LINUX_FLAGS=""
CMAKE_OSX_FLAGS="\
-DARROW_GFLAGS_USE_SHARED=OFF"

if [ "$ARROW_TRAVIS_USE_TOOLCHAIN" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_JEMALLOC=ON"
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_WITH_BZ2=ON"
fi

if [ $only_library_mode == "yes" ]; then
  CMAKE_COMMON_FLAGS="\
    $CMAKE_COMMON_FLAGS \
    -DARROW_BUILD_UTILITIES=OFF"
else
  CMAKE_COMMON_FLAGS="\
    $CMAKE_COMMON_FLAGS \
    -DARROW_BUILD_BENCHMARKS=ON \
    -DARROW_BUILD_TESTS=ON \
    -DARROW_BUILD_EXAMPLES=ON \
    -DARROW_BUILD_UTILITIES=ON"
fi

ARROW_CXXFLAGS=""

# Use Ninja for faster builds when using toolchain
if [ "$ARROW_TRAVIS_USE_TOOLCHAIN" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -GNinja"
  if [ "$DISTRO_CODENAME" != "trusty" ]; then
    # Make sure the toolchain linker (from binutils package) is picked up by clang
    ARROW_CXXFLAGS="$ARROW_CXXFLAGS -B$CPP_TOOLCHAIN/bin"
  fi
  export TRAVIS_MAKE=ninja
elif [ "$using_homebrew" = "yes" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -GNinja"
  export TRAVIS_MAKE=ninja
else
  export TRAVIS_MAKE=make
fi

if [ "$ARROW_TRAVIS_FLIGHT" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_FLIGHT=ON"
fi

if [ "$ARROW_TRAVIS_INTEGRATION" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_BUILD_INTEGRATION=ON"
fi

if [ "$ARROW_TRAVIS_PLASMA" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_PLASMA=ON"
fi

if [ "$ARROW_TRAVIS_PLASMA_JAVA_CLIENT" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_PLASMA_JAVA_CLIENT=ON"
fi

if [ "$ARROW_TRAVIS_ORC" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_ORC=ON"
fi

if [ "$ARROW_TRAVIS_PYTHON" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_PYTHON=ON"
  if [ "$using_homebrew" == "yes" ]; then
    CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS \
-DPYTHON_EXECUTABLE=$(brew --prefix python)/bin/python3"
  fi
fi

if [ "$ARROW_TRAVIS_PARQUET" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS \
-DARROW_PARQUET=ON \
-DPARQUET_BUILD_EXAMPLES=ON \
-DPARQUET_BUILD_EXECUTABLES=ON"
fi

if [ "$ARROW_TRAVIS_GANDIVA" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_GANDIVA=ON"
  if [ "$ARROW_TRAVIS_GANDIVA_JAVA" == "1" ]; then
      CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_GANDIVA_JAVA=ON"
  fi
fi

if [ "$ARROW_TRAVIS_VALGRIND" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_TEST_MEMCHECK=ON"
fi

if [ "$ARROW_USE_ASAN" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_USE_ASAN=ON"
fi

if [ "$ARROW_USE_UBSAN" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_USE_UBSAN=ON"
fi


if [ "$ARROW_TRAVIS_COVERAGE" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_GENERATE_COVERAGE=ON"
fi

if [ "$ARROW_TRAVIS_VERBOSE" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_VERBOSE_THIRDPARTY_BUILD=ON"
fi

if [ "$ARROW_TRAVIS_STATIC_BOOST" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_BOOST_USE_SHARED=OFF"
fi

if [ "$ARROW_TRAVIS_OPTIONAL_INSTALL" == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_OPTIONAL_INSTALL=ON"
fi

if [ "$ARROW_TRAVIS_USE_TOOLCHAIN" == "1" ]; then
  conda activate $CPP_TOOLCHAIN
fi

# conda-forge sets the build flags by default to -02, skip this to speed up the build
export CFLAGS=${CFLAGS//-O2}
export CXXFLAGS=${CXXFLAGS//-O2}

if [ $TRAVIS_OS_NAME == "osx" ]; then
  source $TRAVIS_BUILD_DIR/ci/travis_install_osx_sdk.sh
fi

if [ $TRAVIS_OS_NAME == "linux" ]; then
    cmake $CMAKE_COMMON_FLAGS \
          $CMAKE_LINUX_FLAGS \
          -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
          -DBUILD_WARNING_LEVEL=$ARROW_BUILD_WARNING_LEVEL \
          -DARROW_CXXFLAGS="$ARROW_CXXFLAGS" \
          $ARROW_CPP_DIR
else
    cmake $CMAKE_COMMON_FLAGS \
          $CMAKE_OSX_FLAGS \
          -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
          -DBUILD_WARNING_LEVEL=$ARROW_BUILD_WARNING_LEVEL \
          $ARROW_CPP_DIR
fi

# Build and install libraries. Configure ARROW_CPP_BUILD_TARGETS environment
# variable to only build certain targets. If you use this, you must also set
# the environment variable ARROW_TRAVIS_OPTIONAL_INSTALL=1
time $TRAVIS_MAKE -j4 $ARROW_CPP_BUILD_TARGETS
time $TRAVIS_MAKE install

popd
