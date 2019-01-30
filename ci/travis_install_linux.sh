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

sudo apt-get install -y -qq \
    gdb binutils ccache libboost-dev libboost-filesystem-dev \
    libboost-system-dev libboost-regex-dev

if [ "$CXX" == "g++-4.9" ]; then
    sudo apt-get install -y -qq g++-4.9
fi

if [ "$ARROW_TRAVIS_VALGRIND" == "1" ]; then
    sudo apt-get install -y -qq valgrind
fi

if [ "$ARROW_TRAVIS_COVERAGE" == "1" ]; then
    sudo apt-get install -y -qq lcov
fi

set -x
if [ "$DISTRO_CODENAME" != "trusty" ]; then
    if [ "$ARROW_TRAVIS_GANDIVA" == "1" ]; then
        sudo apt-get install -y -qq llvm-6.0-dev
    fi

    sudo apt-get install -y -qq maven

    # Remove Travis-specific versions of Java
    sudo rm -rf /usr/local/lib/jvm*
    sudo rm -rf /usr/local/maven*
    hash -r
    unset JAVA_HOME

    which java
    which mvn
    java -version
    mvn -v
fi

