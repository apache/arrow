#!/bin/bash

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

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CPP_BUILD_SUPPORT=$SOURCE_DIR/../cpp/build-support

LLVM_VERSION=6.0
CLANG_FORMAT=clang-format-$LLVM_VERSION

$CPP_BUILD_SUPPORT/run_clang_format.py $CLANG_FORMAT \
                                       $CPP_BUILD_SUPPORT/clang_format_exclusions.txt \
                                       $SOURCE_DIR/src --quiet $1
