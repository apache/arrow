#!/bin/bash
set -ex

# Build dependency
export ARROW_HOME=$PREFIX

cd $RECIPE_DIR

if [ "$(uname)" == "Darwin" ]; then
  # C++11 finagling for Mac OSX
  export CC=clang
  export CXX=clang++
  export MACOSX_VERSION_MIN="10.7"
  CXXFLAGS="${CXXFLAGS} -mmacosx-version-min=${MACOSX_VERSION_MIN}"
  CXXFLAGS="${CXXFLAGS} -stdlib=libc++ -std=c++11"
  export LDFLAGS="${LDFLAGS} -mmacosx-version-min=${MACOSX_VERSION_MIN}"
  export LDFLAGS="${LDFLAGS} -stdlib=libc++ -std=c++11"
  export LINKFLAGS="${LDFLAGS}"
  export MACOSX_DEPLOYMENT_TARGET=10.7
fi

echo Setting the compiler...
if [ `uname` == Linux ]; then
  EXTRA_CMAKE_ARGS=-DCMAKE_SHARED_LINKER_FLAGS=-static-libstdc++
elif [ `uname` == Darwin ]; then
  EXTRA_CMAKE_ARGS=
fi

cd ..
$PYTHON setup.py build_ext --extra-cmake-args=$EXTRA_CMAKE_ARGS || exit 1
$PYTHON setup.py install || exit 1
