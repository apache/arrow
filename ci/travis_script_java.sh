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

JAVA_DIR=${TRAVIS_BUILD_DIR}/java
pushd $JAVA_DIR

if [ "$ARROW_TRAVIS_JAVA_BUILD_ONLY" == "1" ]; then
    # Save time and make build less verbose by skipping tests and style checks
    $TRAVIS_MVN -DskipTests=true -Dcheckstyle.skip=true -B install
else
    $TRAVIS_MVN -B install
fi

popd
