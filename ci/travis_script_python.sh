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

source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh

export ARROW_HOME=$ARROW_CPP_INSTALL
export PARQUET_HOME=$ARROW_PYTHON_PARQUET_HOME
export LD_LIBRARY_PATH=$ARROW_HOME/lib:$PARQUET_HOME/lib:$LD_LIBRARY_PATH
export PYARROW_CXXFLAGS="-Werror"

PYTHON_VERSION=$1
CONDA_ENV_DIR=$TRAVIS_BUILD_DIR/pyarrow-test-$PYTHON_VERSION

conda create -y -q -p $CONDA_ENV_DIR python=$PYTHON_VERSION cmake curl
source activate $CONDA_ENV_DIR

python --version
which python

conda install -y -q pip \
      nomkl \
      cloudpickle \
      numpy=1.13.1 \
      pandas \
      cython

if [ "$PYTHON_VERSION" != "2.7" ] || [ $TRAVIS_OS_NAME != "osx" ]; then
  # Install pytorch for torch tensor conversion tests
  # PyTorch seems to be broken on Python 2.7 on macOS so we skip it
  conda install -y -q pytorch torchvision -c soumith
fi

# Build C++ libraries
mkdir -p $ARROW_CPP_BUILD_DIR
pushd $ARROW_CPP_BUILD_DIR

# Clear out prior build files
rm -rf *

cmake -GNinja \
      -DARROW_BUILD_TESTS=off \
      -DARROW_BUILD_UTILITIES=off \
      -DARROW_PLASMA=on \
      -DARROW_PYTHON=on \
      -DARROW_ORC=on \
      -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      $ARROW_CPP_DIR

ninja
ninja install

popd

# Other stuff pip install
pushd $ARROW_PYTHON_DIR

if [ "$PYTHON_VERSION" == "2.7" ]; then
  pip install -q futures
fi

export PYARROW_BUILD_TYPE=$ARROW_BUILD_TYPE

pip install -q -r requirements.txt
python setup.py build_ext --with-parquet --with-plasma --with-orc\
       install -q --single-version-externally-managed --record=record.text
popd

python -c "import pyarrow.parquet"
python -c "import pyarrow.plasma"
python -c "import pyarrow.orc"

if [ $ARROW_TRAVIS_VALGRIND == "1" ]; then
  export PLASMA_VALGRIND=1
fi

PYARROW_PATH=$CONDA_PREFIX/lib/python$PYTHON_VERSION/site-packages/pyarrow
python -m pytest -vv -r sxX --durations=15 -s $PYARROW_PATH --parquet

if [ "$PYTHON_VERSION" == "3.6" ] && [ $TRAVIS_OS_NAME == "linux" ]; then
  # Build documentation once
  conda install -y -q \
        ipython \
        matplotlib \
        numpydoc \
        sphinx \
        sphinx_bootstrap_theme

  pushd $ARROW_PYTHON_DIR/doc
  sphinx-build -q -b html -d _build/doctrees -W source _build/html
  popd
fi
