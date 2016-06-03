#!/usr/bin/env bash

set -e

if [ $TRAVIS_OS_NAME == "linux" ]; then
  MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh"
else
  MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda-latest-MacOSX-x86_64.sh"
fi

wget -O miniconda.sh $MINICONDA_URL
export MINICONDA=$TRAVIS_BUILD_DIR/miniconda
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

