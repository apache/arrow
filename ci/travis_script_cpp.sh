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

set -e

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

pushd $CPP_BUILD_DIR

if [ $TRAVIS_OS_NAME == "osx" ]; then
  # TODO: This does not seem to terminate
  CTEST_ARGS="-E arrow-flight-test"
fi

PATH=$ARROW_BUILD_TYPE:$PATH ctest -j2 --output-on-failure -L unittest ${CTEST_ARGS}

popd

# Capture C++ coverage info (we wipe the build dir in travis_script_python.sh)
if [ "$ARROW_TRAVIS_COVERAGE" == "1" ]; then
    pushd $TRAVIS_BUILD_DIR
    lcov --directory . --capture --no-external --output-file $ARROW_CPP_COVERAGE_FILE \
        2>&1 | grep -v "ignoring data for external file"
    popd
fi
