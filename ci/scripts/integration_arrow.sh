#!/usr/bin/env bash
#
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

set -ex

arrow_dir=${1}
gold_dir=$arrow_dir/testing/data/arrow-ipc-stream/integration

pip install -e $arrow_dir/dev/archery[integration]

#     --run-ipc \
#     --run-flight \

# XXX Can we better integrate this with the rest of the Go build tooling?
pushd ${arrow_dir}/go/arrow/internal/cdata_integration

case "$(uname)" in
    Linux)
        go_lib="arrow_go_integration.so"
        ;;
    Darwin)
        go_lib="arrow_go_integration.so"
        ;;
    MINGW*)
        go_lib="arrow_go_integration.dll"
        ;;
esac

go build -tags cdata_integration,assert -buildmode=c-shared -o ${go_lib} .

popd

# Rust can be enabled by exporting ARCHERY_INTEGRATION_WITH_RUST=1
time archery integration \
    --run-c-data \
    --with-cpp=1 \
    --with-csharp=0 \
    --with-java=0 \
    --with-js=0 \
    --with-go=1 \
    --gold-dirs=$gold_dir/0.14.1 \
    --gold-dirs=$gold_dir/0.17.1 \
    --gold-dirs=$gold_dir/1.0.0-bigendian \
    --gold-dirs=$gold_dir/1.0.0-littleendian \
    --gold-dirs=$gold_dir/2.0.0-compression \
    --gold-dirs=$gold_dir/4.0.0-shareddict \
