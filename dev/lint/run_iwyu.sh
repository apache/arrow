#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -eux

mkdir -p /build/lint
pushd /build/lint

cmake -GNinja \
      -DCMAKE_BUILD_TYPE=debug \
      -DARROW_FLIGHT=ON \
      -DARROW_GANDIVA=ON \
      -DARROW_PARQUET=ON \
      -DARROW_PYTHON=ON \
      -DCMAKE_CXX_FLAGS='-D_GLIBCXX_USE_CXX11_ABI=0' \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
      /arrow/cpp

popd

export IWYU_COMPILATION_DATABASE_PATH=/build/lint
/arrow/cpp/build-support/iwyu/iwyu.sh all
