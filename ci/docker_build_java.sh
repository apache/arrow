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

export MAVEN_OPTS="-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"

# /arrow/java is read-only
mkdir -p /build/java

arrow_src=/build/java/arrow

# Remove any pre-existing artifacts
rm -rf $arrow_src

pushd /arrow
rsync -a header java format integration testing $arrow_src
popd

JAVA_ARGS=
if [ "$ARROW_JAVA_RUN_TESTS" != "1" ]; then
  JAVA_ARGS=-DskipTests
fi

if [ "$ARROW_JAVA_SHADE_FLATBUFS" == "1" ]; then
  SHADE_FLATBUFFERS=-Pshade-flatbuffers
fi

pushd $arrow_src/java
mvn -B $JAVA_ARGS -Drat.skip=true install $SHADE_FLATBUFFERS

if [ "$ARROW_JAVADOC" == "1" ]; then
  export MAVEN_OPTS="$MAVEN_OPTS -Dorg.slf4j.simpleLogger.defaultLogLevel=warn"
  mvn -B site
fi
popd
