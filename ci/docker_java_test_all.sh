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

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export ARROW_TEST_DATA=/arrow/testing/data

export ARROW_JAVA_RUN_TESTS=1

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export ARROW_JAVADOC=1
bash $SOURCE_DIR/docker_build_java.sh

export ARROW_JAVA_SHADE_FLATBUFS=1
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export ARROW_JAVADOC=0
bash $SOURCE_DIR/docker_build_java.sh
