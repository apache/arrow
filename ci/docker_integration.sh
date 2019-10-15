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

set -eu

BIN_DIR=/bin
CPP_DIR=/build/cpp
JAVA_DIR=/build/java
JS_DIR=/build/js

# Copy various stuff into our Docker build directory
pushd /arrow
rsync -r \
      LICENSE.txt NOTICE.txt \
      header java js format integration testing /build
popd

mkdir -p $BIN_DIR
mkdir -p $CPP_DIR

# Install archery tool, require writable -e install
cp -r /arrow/dev/archery /archery
pip install -e /archery

# archery requires NumPy
pip install numpy

# Archery integration test environment variables
export ARROW_ROOT=/build
export ARROW_TEST_DATA=/arrow/testing/data

# Make a minimal C++ build for running integration tests
function build_cpp() {
  pushd $CPP_DIR

  cmake /arrow/cpp \
        -DCMAKE_BUILD_TYPE=debug \
        -DBOOST_SOURCE=SYSTEM \
        -DARROW_BUILD_INTEGRATION=ON \
        -DARROW_FLIGHT=ON \
        -DARROW_COMPUTE=OFF \
        -DARROW_DATASET=OFF \
        -DARROW_FILESYSTEM=OFF \
        -DARROW_HDFS=OFF \
        -DARROW_JEMALLOC=OFF \
        -DARROW_JSON=OFF \
        -DARROW_USE_GLOG=OFF

  make -j4 arrow-integration
  export ARROW_CPP_EXE_PATH=$CPP_DIR/debug
}

function build_java() {
  export MAVEN_OPTS="-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"

  export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

  # /arrow/java is read-only
  pushd /arrow
  popd

  pushd $JAVA_DIR
  mvn package -B -DskipTests -Drat.skip=true

  popd
}

# Build Go
function build_go() {
  pushd /arrow/go/arrow

  export GO111MODULE=on
  export GOBIN=$BIN_DIR
  which go
  go version
  go env
  go get -v ./...

  popd
}

# Build JS
function build_js() {
  source $NVM_DIR/nvm.sh
  nvm install $NODE_VERSION
  nvm alias default $NODE_VERSION
  nvm use default

  pushd $JS_DIR

  npm install
  npm run build -- -t apache-arrow

  popd
}

# Run integration tests
GOLD_14_1=$ARROW_TEST_DATA/arrow-ipc/integration/0.14.1

build_cpp
build_go
build_js
build_java

archery integration --with-all --run-flight
