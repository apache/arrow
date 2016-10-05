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

set -ex

source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh

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
