#!/usr/bin/env bash

set -e

PYTHON_DIR=$TRAVIS_BUILD_DIR/python

# Re-use conda installation from C++
export MINICONDA=$TRAVIS_BUILD_DIR/miniconda
export PATH="$MINICONDA/bin:$PATH"
export LD_LIBRARY_PATH="$MINICONDA/lib:$LD_LIBRARY_PATH"
export PARQUET_HOME=$MINICONDA

# Share environment with C++
pushd $CPP_BUILD_DIR
source setup_build_env.sh
popd

pushd $PYTHON_DIR

python_version_tests() {
  PYTHON_VERSION=$1
  CONDA_ENV_NAME="pyarrow-test-${PYTHON_VERSION}"
  conda create -y -q -n $CONDA_ENV_NAME python=$PYTHON_VERSION
  source activate $CONDA_ENV_NAME

  python --version
  which python

  # faster builds, please
  conda install -y nomkl

  # Expensive dependencies install from Continuum package repo
  conda install -y pip numpy pandas cython

  # Other stuff pip install
  pip install -r requirements.txt

  export ARROW_HOME=$ARROW_CPP_INSTALL

  python setup.py build_ext --inplace

  python -m pytest -vv -r sxX pyarrow
}

# run tests for python 2.7 and 3.5
python_version_tests 2.7
python_version_tests 3.5

popd
