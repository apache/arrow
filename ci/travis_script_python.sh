#!/usr/bin/env bash

set -e

PYTHON_DIR=$TRAVIS_BUILD_DIR/python

# Share environment with C++
pushd $CPP_BUILD_DIR
source setup_build_env.sh
popd

pushd $PYTHON_DIR

# Bootstrap a Conda Python environment

if [ $TRAVIS_OS_NAME == "linux" ]; then
  MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh"
else
  MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda-latest-MacOSX-x86_64.sh"
fi

curl $MINICONDA_URL > miniconda.sh
MINICONDA=$TRAVIS_BUILD_DIR/miniconda
bash miniconda.sh -b -p $MINICONDA
export PATH="$MINICONDA/bin:$PATH"
conda update -y -q conda
conda info -a

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
