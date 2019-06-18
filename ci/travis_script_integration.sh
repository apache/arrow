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

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh

export ARROW_CPP_EXE_PATH=$ARROW_CPP_BUILD_DIR/debug

pushd $ARROW_JAVA_DIR

echo "mvn package"
$TRAVIS_MVN -B clean package 2>&1 > mvn_package.log || (cat mvn_package.log && false)

popd

pushd $ARROW_JS_DIR

# lint and compile JS source
npm run lint
npm run build -- -t apache-arrow

popd

pushd $ARROW_INTEGRATION_DIR

conda activate $CPP_TOOLCHAIN
# Install integration test requirements
conda install -y -q python=3.6 six numpy

# ARROW-4008: Create a directory to write temporary files since /tmp can be
# unstable in Travis CI
INTEGRATION_TEMPDIR=$TRAVIS_BUILD_DIR/integration_temp
mkdir -p $INTEGRATION_TEMPDIR

python integration_test.py --debug --tempdir=$INTEGRATION_TEMPDIR --run_flight

popd

# pushd $ARROW_JS_DIR

# run tests against source to generate coverage data
# npm run test:coverage
# Uncomment to upload to coveralls
# cat ./coverage/lcov.info | ./node_modules/coveralls/bin/coveralls.js;

# popd
