#!/usr/bin/env bash

set -e

if [ $TRAVIS_OS_NAME == "linux" ]; then
  MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh"
else
  MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda-latest-MacOSX-x86_64.sh"
fi

wget -O miniconda.sh $MINICONDA_URL
MINICONDA=$TRAVIS_BUILD_DIR/miniconda
bash miniconda.sh -b -p $MINICONDA
export PATH="$MINICONDA/bin:$PATH"
conda update -y -q conda
conda info -a

conda config --set show_channel_urls yes
conda config --add channels conda-forge
conda config --add channels apache

conda install --yes conda-build jinja2 anaconda-client

# faster builds, please
conda install -y nomkl

# Build libarrow

cd $TRAVIS_BUILD_DIR/cpp

conda build conda.recipe --channel apache/channel/dev
CONDA_PACKAGE=`conda build --output conda.recipe | grep bz2`

if [ $TRAVIS_BRANCH == "master" ] && [ $TRAVIS_PULL_REQUEST == "false" ]; then
  anaconda --token $ANACONDA_TOKEN upload $CONDA_PACKAGE --user apache --channel dev;
fi

# Build pyarrow

cd $TRAVIS_BUILD_DIR/python

build_for_python_version() {
  PY_VERSION=$1
  conda build conda.recipe --python $PY_VERSION --channel apache/channel/dev
  CONDA_PACKAGE=`conda build --python $PY_VERSION --output conda.recipe | grep bz2`

  if [ $TRAVIS_BRANCH == "master" ] && [ $TRAVIS_PULL_REQUEST == "false" ]; then
	anaconda --token $ANACONDA_TOKEN upload $CONDA_PACKAGE --user apache --channel dev;
  fi
}

build_for_python_version 2.7
build_for_python_version 3.5
