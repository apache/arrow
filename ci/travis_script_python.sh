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

PYTHON_DIR=$TRAVIS_BUILD_DIR/python

# Re-use conda installation from C++
export MINICONDA=$HOME/miniconda
export PATH="$MINICONDA/bin:$PATH"
export PARQUET_HOME=$MINICONDA

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

  # conda install -y parquet-cpp

  conda install -y arrow-cpp -c apache/channel/dev

  # Other stuff pip install
  pip install -r requirements.txt

  export ARROW_HOME=$ARROW_CPP_INSTALL

  python setup.py build_ext \
		 --inplace

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
