#!/usr/bin/env bash

arrow_dir=$1
build_dir=$2

# Multibuild specific environment variables
export PLAT=arm64
export MB_PYTHON_VERSION=3.9
export CONFIG_PATH=/dev/null

# If we don't set BUILD_PREFIX to a non-existing location then 
# multibuild will set it to /usr/local causing a pretty 
# misleading cmake issue.
# CMake tries to locate numpy's include path by importing it 
# while swallowing any errors. If BUILD_PREFIX is set to 
# /usr/local (by default) then multibuild will prepend PATH 
# with /usr/local/bin which is the x86_64 brew installation 
# prefix. This causes the shell script to call the x86_64 
# variant of CMake instead of the arm64 one located under
# /opt/homebrew/bin which calls the universal2 python binary 
# as x86_64 where importing an arm64 compiled numpy will fail. 
# Since CMake swallows the errors it is unable to locate numpy 
# both when compiling libarrow_python and pyarrow.
export BUILD_PREFIX=$build_dir

# In order to produce arm64 platform tag instead of universal2
export _PYTHON_HOST_PLATFORM="macosx-11.0-arm64"
export ARCHFLAGS="-arch arm64"
export MACOSX_DEPLOYMENT_TARGET=11.0

# Arrow specific environment variables
export CMAKE_BUILD_TYPE=debug
export PYARROW_BUILD_VERBOSE=1
export PYTHON_VERSION=3.9
export VCPKG_DEFAULT_TRIPLET=arm64-osx-static-release
export VCPKG_FEATURE_FLAGS=-manifests
export VCPKG_OVERLAY_TRIPLETS=/Users/ursa/kszucs/arrow/ci/vcpkg
export VCPKG_ROOT=/Users/ursa/kszucs/vcpkg

# Unsupported arrow features
export ARROW_FLIGHT=OFF
export ARROW_JEMALLOC=OFF
export ARROW_SIMD_LEVEL=NONE

source $arrow_dir/../multibuild/travis_osx_steps.sh
before_install

pip install numpy delocate cython setuptools_scm wheel

rm -rf build
$arrow_dir/ci/scripts/python_wheel_macos_build.sh $arrow_dir $build_dir