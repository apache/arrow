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

use_gcc() {
  export CC=gcc-4.9
  export CXX=g++-4.9
}

use_clang() {
  export CC=clang-4.0
  export CXX=clang++-4.0
}

bootstrap_python_env() {
  PYTHON_VERSION=$1
  CONDA_ENV_DIR=$BUILD_DIR/pyarrow-test-$PYTHON_VERSION

  conda create -y -q -p $CONDA_ENV_DIR python=$PYTHON_VERSION cmake curl
  conda activate $CONDA_ENV_DIR

  python --version
  which python

  # faster builds, please
  conda install -y -q nomkl pip numpy pandas cython
}

build_pyarrow() {
  # Other stuff pip install
  pushd $ARROW_PYTHON_DIR
  pip install -r requirements.txt
  python setup.py build_ext --with-parquet --with-plasma \
         install --single-version-externally-managed --record=record.text
  popd

  python -c "import pyarrow.parquet"
  python -c "import pyarrow.plasma"

  export PYARROW_PATH=$CONDA_PREFIX/lib/python$PYTHON_VERSION/site-packages/pyarrow
}

build_arrow() {
  mkdir -p $ARROW_CPP_BUILD_DIR
  pushd $ARROW_CPP_BUILD_DIR

  cmake -GNinja \
        -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
        -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
        -DARROW_NO_DEPRECATED_API=ON \
        -DARROW_PARQUET=ON \
        -DARROW_PYTHON=ON \
        -DARROW_PLASMA=ON \
        -DARROW_BOOST_USE_SHARED=off \
        $ARROW_CPP_DIR

  ninja
  ninja install
  popd
}
