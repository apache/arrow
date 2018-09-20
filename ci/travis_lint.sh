#!/usr/bin/env bash

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

set -ex

# Disable toolchain variables in this script
export ARROW_TRAVIS_USE_TOOLCHAIN=0
source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

# Fail fast for code linting issues

if [ "$ARROW_CI_CPP_AFFECTED" != "0" ]; then
  mkdir $ARROW_CPP_DIR/lint
  pushd $ARROW_CPP_DIR/lint

  cmake ..
  make lint

  if [ "$ARROW_TRAVIS_CLANG_FORMAT" == "1" ]; then
    make check-format
  fi

  python $ARROW_CPP_DIR/build-support/lint_cpp_cli.py $ARROW_CPP_DIR/src

  popd
fi


# Fail fast on style checks
if [ "$ARROW_CI_DEV_AFFECTED" != "0" ]; then
  # crossbow requires python3
  sudo apt-get install -y -q python3-pip
  sudo pip3 install -q flake8
  python3 -m flake8 --count $ARROW_CROSSBOW_DIR
fi

if [ "$ARROW_CI_INTEGRATION_AFFECTED" != "0" ]; then
  sudo pip install -q flake8
  python -m flake8 --count $ARROW_INTEGRATION_DIR
fi

if [ "$ARROW_CI_PYTHON_AFFECTED" != "0" ]; then
  sudo pip install -q flake8

  python -m flake8 --count $ARROW_PYTHON_DIR

  # Check Cython files with some checks turned off
  python -m flake8 --count \
                   --config=$ARROW_PYTHON_DIR/.flake8.cython \
                   $ARROW_PYTHON_DIR
fi
