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

: ${CPP_BUILD_DIR=$TRAVIS_BUILD_DIR/cpp-build}

JAVA_DIR=${TRAVIS_BUILD_DIR}/java

pushd $JAVA_DIR

mvn package

popd

pushd $TRAVIS_BUILD_DIR/integration

VERSION=0.1.1-SNAPSHOT
export ARROW_JAVA_INTEGRATION_JAR=$JAVA_DIR/tools/target/arrow-tools-$VERSION-jar-with-dependencies.jar
export ARROW_CPP_TESTER=$CPP_BUILD_DIR/debug/json-integration-test

source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh
export MINICONDA=$HOME/miniconda
export PATH="$MINICONDA/bin:$PATH"

CONDA_ENV_NAME=arrow-integration-test
conda create -y -q -n $CONDA_ENV_NAME python=3.5
source activate $CONDA_ENV_NAME

# faster builds, please
conda install -y nomkl

# Expensive dependencies install from Continuum package repo
conda install -y pip numpy six

python integration_test.py --debug

popd
