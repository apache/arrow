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

source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh

PYTHON_DIR=$TRAVIS_BUILD_DIR/python

# Re-use conda installation from C++
export MINICONDA=$HOME/miniconda
export PATH="$MINICONDA/bin:$PATH"

export ARROW_HOME=$ARROW_CPP_INSTALL
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ARROW_CPP_INSTALL/lib

pushd $PYTHON_DIR
export PARQUET_HOME=$TRAVIS_BUILD_DIR/parquet-env

build_parquet_cpp() {
  conda create -y -q -p $PARQUET_HOME thrift-cpp snappy zlib brotli boost
  source activate $PARQUET_HOME

  export BOOST_ROOT=$PARQUET_HOME
  export SNAPPY_HOME=$PARQUET_HOME
  export THRIFT_HOME=$PARQUET_HOME
  export ZLIB_HOME=$PARQUET_HOME
  export BROTLI_HOME=$PARQUET_HOME

  PARQUET_DIR=$TRAVIS_BUILD_DIR/parquet
  mkdir -p $PARQUET_DIR

  git clone https://github.com/apache/parquet-cpp.git $PARQUET_DIR

  pushd $PARQUET_DIR
  mkdir build-dir
  cd build-dir

  cmake \
      -DCMAKE_BUILD_TYPE=debug \
      -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
      -DPARQUET_ARROW=on \
      -DPARQUET_BUILD_BENCHMARKS=off \
      -DPARQUET_BUILD_EXECUTABLES=off \
      -DPARQUET_ZLIB_VENDORED=off \
      -DPARQUET_BUILD_TESTS=off \
      ..

  make -j${CPU_COUNT}
  make install

  popd
}

build_parquet_cpp

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PARQUET_HOME/lib

python_version_tests() {
  PYTHON_VERSION=$1
  CONDA_ENV_DIR=$TRAVIS_BUILD_DIR/pyarrow-test-$PYTHON_VERSION
  conda create -y -q -p $CONDA_ENV_DIR python=$PYTHON_VERSION
  source activate $CONDA_ENV_DIR

  python --version
  which python

  # faster builds, please
  conda install -y nomkl

  # Expensive dependencies install from Continuum package repo
  conda install -y pip numpy pandas cython

  # Other stuff pip install
  pip install -r requirements.txt

  python setup.py build_ext --inplace --with-parquet

  python -c "import pyarrow.parquet"

  python -m pytest -vv -r sxX pyarrow

  # Build documentation once
  if [[ "$PYTHON_VERSION" == "3.5" ]]
  then
      pip install -r doc/requirements.txt
      python setup.py build_sphinx
  fi
}

# run tests for python 2.7 and 3.5
python_version_tests 2.7
python_version_tests 3.5

popd
