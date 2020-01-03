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

source_dir=${1}/ruby

export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}
export PKG_CONFIG_PATH=${ARROW_HOME}/lib/pkgconfig
export GI_TYPELIB_PATH=${ARROW_HOME}/lib/girepository-1.0

# TODO(kszucs): this should be moved into the dockerfile
bundle install --gemfile ${source_dir}/red-arrow/Gemfile

${source_dir}/red-arrow/test/run-test.rb

if [ ${ARROW_GANDIVA:-OFF} == "ON" ]; then
    ${source_dir}/red-gandiva/test/run-test.rb
fi
if [ ${ARROW_PARQUET:-OFF} == "ON" ]; then
    ${source_dir}/red-parquet/test/run-test.rb
fi
if [ ${ARROW_PLASMA:-OFF} == "ON" ]; then
    ${source_dir}/red-plasma/test/run-test.rb
fi
if [ ${ARROW_CUDA:-OFF} == "ON" ]; then
    ${source_dir}/red-arrow-cuda/test/run-test.rb
fi
