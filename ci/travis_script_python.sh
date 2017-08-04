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

set -e

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

export ARROW_HOME=$ARROW_CPP_INSTALL
export PARQUET_HOME=$TRAVIS_BUILD_DIR/parquet-env
export LD_LIBRARY_PATH=$ARROW_HOME/lib:$PARQUET_HOME/lib:$LD_LIBRARY_PATH
export PYARROW_CXXFLAGS="-Werror"

build_parquet_cpp() {
  export PARQUET_ARROW_VERSION=$(git rev-parse HEAD)

  # $CPP_TOOLCHAIN set up in before_script_cpp
  export PARQUET_BUILD_TOOLCHAIN=$CPP_TOOLCHAIN

  PARQUET_DIR=$TRAVIS_BUILD_DIR/parquet
  mkdir -p $PARQUET_DIR

  git clone https://github.com/apache/parquet-cpp.git $PARQUET_DIR

  pushd $PARQUET_DIR
  mkdir build-dir
  cd build-dir

  cmake \
      -GNinja \
      -DCMAKE_BUILD_TYPE=debug \
      -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
      -DPARQUET_BOOST_USE_SHARED=off \
      -DPARQUET_BUILD_BENCHMARKS=off \
      -DPARQUET_BUILD_EXECUTABLES=off \
      -DPARQUET_BUILD_TESTS=off \
      ..

  ninja
  ninja install

  popd
}

build_parquet_cpp

function rebuild_arrow_libraries() {
  pushd $ARROW_CPP_BUILD_DIR

  # Clear out prior build files
  rm -rf *

  cmake -GNinja \
        -DARROW_BUILD_TESTS=off \
        -DARROW_BUILD_UTILITIES=off \
        -DARROW_PLASMA=on \
        -DARROW_PYTHON=on \
        -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
        $ARROW_CPP_DIR

  ninja
  ninja install

  popd
}

python_version_tests() {
  PYTHON_VERSION=$1
  CONDA_ENV_DIR=$TRAVIS_BUILD_DIR/pyarrow-test-$PYTHON_VERSION

  conda create -y -q -p $CONDA_ENV_DIR python=$PYTHON_VERSION cmake curl
  source activate $CONDA_ENV_DIR

  python --version
  which python

  # faster builds, please
  conda install -y -q nomkl

  # Expensive dependencies install from Continuum package repo
  conda install -y -q pip numpy pandas cython flake8

  # Fail fast on style checks
  flake8 pyarrow

  # Check Cython files with some checks turned off
  flake8 --config=.flake8.cython pyarrow

  # Build C++ libraries
  rebuild_arrow_libraries

  # Other stuff pip install
  pushd $ARROW_PYTHON_DIR
  pip install -r requirements.txt
  python setup.py build_ext --with-parquet --with-plasma \
         install --single-version-externally-managed --record=record.text
  popd

  python -c "import pyarrow.parquet"
  python -c "import pyarrow.plasma"

  if [ $TRAVIS_OS_NAME == "linux" ]; then
    export PLASMA_VALGRIND=1
  fi

  PYARROW_PATH=$CONDA_PREFIX/lib/python$PYTHON_VERSION/site-packages/pyarrow
  python -m pytest -vv -r sxX -s $PYARROW_PATH --parquet

  if [ "$PYTHON_VERSION" == "3.6" ] && [ $TRAVIS_OS_NAME == "linux" ]; then
      # Build documentation once
      pushd $ARROW_PYTHON_DIR/doc
      conda install -y -q --file=requirements.txt
      sphinx-build -b html -d _build/doctrees -W source _build/html
      popd
  fi
}

# run tests for python 2.7 and 3.6
python_version_tests 2.7
python_version_tests 3.6
