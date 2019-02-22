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

export MAVEN_OPTS="-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"

pushd $JAVA_DIR

# build with gandiva profile
$TRAVIS_MVN -P gandiva -B install -DskipTests -Dgandiva.cpp.build.dir=$CPP_BUILD_DIR/debug

# run gandiva tests
$TRAVIS_MVN test -P gandiva -pl gandiva -Dgandiva.cpp.build.dir=$CPP_BUILD_DIR/debug

popd
