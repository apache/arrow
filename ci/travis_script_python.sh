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

pushd $PYTHON_DIR
export PARQUET_HOME=$TRAVIS_BUILD_DIR/parquet-env

build_parquet_cpp() {
  conda create -y -q -p $PARQUET_HOME python=3.6
  source activate $PARQUET_HOME

  # In case some package wants to download the MKL
  conda install -y -q nomkl

  conda install -y -q thrift-cpp snappy zlib brotli boost

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

function build_arrow_libraries() {
  CPP_BUILD_DIR=$1
  CPP_DIR=$TRAVIS_BUILD_DIR/cpp

  mkdir $CPP_BUILD_DIR
  pushd $CPP_BUILD_DIR

  cmake -DARROW_BUILD_TESTS=off \
        -DARROW_PYTHON=on \
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

  conda create -y -q -p $CONDA_ENV_DIR python=$PYTHON_VERSION
  source activate $CONDA_ENV_DIR

  python --version
  which python

  # faster builds, please
  conda install -y nomkl

  # Expensive dependencies install from Continuum package repo
  conda install -y pip numpy pandas cython

  # Build C++ libraries
  build_arrow_libraries arrow-build-$PYTHON_VERSION $ARROW_HOME

  # Other stuff pip install
  pip install -r requirements.txt

  python setup.py build_ext --inplace --with-parquet --with-jemalloc

  python -c "import pyarrow.parquet"
  python -c "import pyarrow.jemalloc"

  python -m pytest -vv -r sxX pyarrow

  # Build documentation once
  if [[ "$PYTHON_VERSION" == "3.6" ]]
  then
      pip install -r doc/requirements.txt
      python setup.py build_sphinx
  fi
}

# run tests for python 2.7 and 3.6
python_version_tests 2.7
python_version_tests 3.6

popd
