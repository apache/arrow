#!/usr/bin/env bash

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

export MINICONDA=$HOME/miniconda
export PATH="$MINICONDA/bin:$PATH"
export CONDA_PKGS_DIRS=$HOME/.conda_packages

export ARROW_CPP_DIR=$TRAVIS_BUILD_DIR/cpp
export ARROW_PYTHON_DIR=$TRAVIS_BUILD_DIR/python
export ARROW_C_GLIB_DIR=$TRAVIS_BUILD_DIR/c_glib
export ARROW_JAVA_DIR=${TRAVIS_BUILD_DIR}/java
export ARROW_JS_DIR=${TRAVIS_BUILD_DIR}/js
export ARROW_INTEGRATION_DIR=$TRAVIS_BUILD_DIR/integration

export CPP_BUILD_DIR=$TRAVIS_BUILD_DIR/cpp-build

export ARROW_CPP_INSTALL=$TRAVIS_BUILD_DIR/cpp-install
export ARROW_CPP_BUILD_DIR=$TRAVIS_BUILD_DIR/cpp-build
export ARROW_C_GLIB_INSTALL=$TRAVIS_BUILD_DIR/c-glib-install

if [ "$ARROW_TRAVIS_USE_TOOLCHAIN" == "1" ]; then
  # C++ toolchain
  export CPP_TOOLCHAIN=$TRAVIS_BUILD_DIR/cpp-toolchain
  export ARROW_BUILD_TOOLCHAIN=$CPP_TOOLCHAIN
  export BOOST_ROOT=$CPP_TOOLCHAIN

  export PATH=$CPP_TOOLCHAIN/bin:$PATH
  export LD_LIBRARY_PATH=$CPP_TOOLCHAIN/lib:$LD_LIBRARY_PATH
  export TRAVIS_MAKE=ninja
else
  export TRAVIS_MAKE=make
fi

if [ $TRAVIS_OS_NAME == "osx" ]; then
  export GOPATH=$TRAVIS_BUILD_DIR/gopath
fi
