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

set -e

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

export ARROW_HOME=$ARROW_CPP_INSTALL
export PYARROW_WITH_PLASMA=1

pushd $ARROW_PYTHON_DIR

function build_arrow_libraries() {
  CPP_BUILD_DIR=$1
  CPP_DIR=$TRAVIS_BUILD_DIR/cpp

  mkdir $CPP_BUILD_DIR
  pushd $CPP_BUILD_DIR

  cmake -DARROW_BUILD_TESTS=off \
        -DARROW_PYTHON=on \
        -DARROW_PLASMA=on \
        -DCMAKE_INSTALL_PREFIX=$2 \
        $CPP_DIR

  make -j4
  make install

  popd
}

python_version_tests() {
  PYTHON_VERSION=$1
  CONDA_ENV_DIR=$TRAVIS_BUILD_DIR/pyarrow-test-$PYTHON_VERSION

  export ARROW_HOME=$TRAVIS_BUILD_DIR/arrow-install-$PYTHON_VERSION
  export LD_LIBRARY_PATH=$ARROW_HOME/lib:$PARQUET_HOME/lib

  conda create -y -q -p $CONDA_ENV_DIR python=$PYTHON_VERSION cmake curl
  source activate $CONDA_ENV_DIR

  python --version
  which python

  # faster builds, please
  conda install -y -q nomkl

  # Expensive dependencies install from Continuum package repo
  conda install -y -q pip numpy pandas cython

  # Build C++ libraries
  build_arrow_libraries arrow-build-$PYTHON_VERSION $ARROW_HOME

  # Other stuff pip install
  pip install -r requirements.txt

  python setup.py build_ext --inplace

  python -m pytest -vv -r sxX pyarrow

  # Build documentation once
  if [[ "$PYTHON_VERSION" == "3.6" ]]
  then
      conda install -y -q --file=doc/requirements.txt
      python setup.py build_sphinx -s doc/source
  fi

  # Build and install pyarrow
  pushd $TRAVIS_BUILD_DIR/python
    python setup.py install
  popd

  # Run Plasma tests
  pushd $TRAVIS_BUILD_DIR/python
    python -m pytest pyarrow/tests/test_plasma.py
    if [ $TRAVIS_OS_NAME == "linux" ]; then
      PLASMA_VALGRIND=1 python -m pytest pyarrow/tests/test_plasma.py
    fi
  popd
}

# run tests for python 2.7 and 3.6
python_version_tests 2.7
python_version_tests 3.6

popd
