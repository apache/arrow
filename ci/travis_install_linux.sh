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

sudo apt-get install -y -q \
    gdb binutils ccache libboost-dev libboost-filesystem-dev \
    libboost-system-dev libboost-regex-dev

if [ "$CXX" == "g++-4.9" ]; then
    sudo apt-get install -y -q g++-4.9
fi

if [ "$ARROW_TRAVIS_VALGRIND" == "1" ]; then
    sudo apt-get install -y -q valgrind
fi

if [ "$ARROW_TRAVIS_COVERAGE" == "1" ]; then
    sudo apt-get install -y -q lcov
fi
