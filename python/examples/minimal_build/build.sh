#!/bin/bash
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

# Set $ARROW_ROOT to the path of your Arrow clone and run

# docker build -t arrow_python_minimal .
# docker run --rm -t -i -v $PWD:/io -v $ARROW_ROOT:/arrow  arrow_python_minimal /io/build.sh

set -ex

#----------------------------------------------------------------------
# Change this to whatever makes sense for your system

HOME=
MINICONDA=$HOME/miniconda-for-arrow
LIBRARY_INSTALL_DIR=$HOME/local-libs
CPP_BUILD_DIR=$HOME/arrow-cpp-build
ARROW_ROOT=/arrow
PYTHON=3.7

#----------------------------------------------------------------------
# Run these only once

function setup_miniconda() {
  MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh"
  wget -O miniconda.sh $MINICONDA_URL
  bash miniconda.sh -b -p $MINICONDA
  rm -f miniconda.sh
  LOCAL_PATH=$PATH
  export PATH="$MINICONDA/bin:$PATH"

  conda update -y -q conda
  conda config --set auto_update_conda false
  conda info -a

  conda config --set show_channel_urls True
  conda config --add channels https://repo.continuum.io/pkgs/free
  conda config --add channels conda-forge

  export PATH=$LOCAL_PATH
}

function create_conda_environment() {
  conda create -y -n pyarrow-$PYTHON -c conda-forge \
        --file arrow/ci/conda_env_unix.yml \
        --file arrow/ci/conda_env_cpp.yml \
        --file arrow/ci/conda_env_python.yml \
        --file arrow/ci/conda_env_gandiva.yml \
        compilers \
        python=3.7 \
        pandas
}

setup_miniconda
create_conda_environment

#----------------------------------------------------------------------
# Activate conda in bash and activate conda environment

. $MINICONDA/etc/profile.d/conda.sh
conda activate pyarrow-$PYTHON
export ARROW_HOME=$CONDA_PREFIX

#----------------------------------------------------------------------
# Build C++ library
NPROC=$(nproc)

mkdir -p $CPP_BUILD_DIR
pushd $CPP_BUILD_DIR

cmake -GNinja \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      -DCMAKE_INSTALL_LIBDIR=lib \
      -DARROW_FLIGHT=ON \
      -DARROW_GANDIVA=ON \
      -DARROW_ORC=ON \
      -DARROW_WITH_BZ2=ON \
      -DARROW_WITH_ZLIB=ON \
      -DARROW_WITH_ZSTD=ON \
      -DARROW_WITH_LZ4=ON \
      -DARROW_WITH_SNAPPY=ON \
      -DARROW_WITH_BROTLI=ON \
      -DARROW_PARQUET=ON \
      -DARROW_PYTHON=ON \
      -DARROW_PLASMA=ON \
      -DARROW_BUILD_TESTS=ON \
      ..

ninja install

popd

#----------------------------------------------------------------------
# Build and test Python library
pushd $ARROW_ROOT/python

rm -rf build/  # remove any pesky pre-existing build directory

export PYARROW_WITH_FLIGHT=1
export PYARROW_WITH_GANDIVA=1
export PYARROW_WITH_ORC=1
export PYARROW_WITH_PARQUET=1

python setup.py build_ext --inplace
python setup.py develop

py.test pyarrow
