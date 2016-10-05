#!/bin/bash

#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.

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

# echo Setting the compiler...
# if [ `uname` == Linux ]; then
#   EXTRA_CMAKE_ARGS=-DCMAKE_SHARED_LINKER_FLAGS=-static-libstdc++
# elif [ `uname` == Darwin ]; then
#   EXTRA_CMAKE_ARGS=
# fi

cd ..
# $PYTHON setup.py build_ext --extra-cmake-args=$EXTRA_CMAKE_ARGS || exit 1
$PYTHON setup.py build_ext || exit 1
$PYTHON setup.py install || exit 1
