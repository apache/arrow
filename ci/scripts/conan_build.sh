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

set -eux

source_dir=${1}
shift
build_dir=${1}
shift

export ARROW_HOME=${source_dir}
export CONAN_HOOK_ERROR_LEVEL=40

conan_args=()
conan_args+=(--build=missing)
if [ -n "${ARROW_CONAN_PARQUET:-}" ]; then
  conan_args+=(--options arrow:parquet=${ARROW_CONAN_PARQUET})
fi
if [ -n "${ARROW_CONAN_WITH_BROTLI:-}" ]; then
  conan_args+=(--options arrow:with_brotli=${ARROW_CONAN_WITH_BROTLI})
fi
if [ -n "${ARROW_CONAN_WITH_BZ2:-}" ]; then
  conan_args+=(--options arrow:with_bz2=${ARROW_CONAN_WITH_BZ2})
fi
if [ -n "${ARROW_CONAN_WITH_FLIGHT_RPC:-}" ]; then
  conan_args+=(--options arrow:with_flight_rpc=${ARROW_CONAN_WITH_FLIGHT_RPC})
fi
if [ -n "${ARROW_CONAN_WITH_GLOG:-}" ]; then
  conan_args+=(--options arrow:with_glog=${ARROW_CONAN_WITH_GLOG})
fi
if [ -n "${ARROW_CONAN_WITH_JEMALLOC:-}" ]; then
  conan_args+=(--options arrow:with_jemalloc=${ARROW_CONAN_WITH_JEMALLOC})
fi
if [ -n "${ARROW_CONAN_WITH_JSON:-}" ]; then
  conan_args+=(--options arrow:with_json=${ARROW_CONAN_WITH_JSON})
fi
if [ -n "${ARROW_CONAN_WITH_LZ4:-}" ]; then
  conan_args+=(--options arrow:with_lz4=${ARROW_CONAN_WITH_LZ4})
fi
if [ -n "${ARROW_CONAN_WITH_SNAPPY:-}" ]; then
  conan_args+=(--options arrow:with_snappy=${ARROW_CONAN_WITH_SNAPPY})
fi
if [ -n "${ARROW_CONAN_WITH_ZSTD:-}" ]; then
  conan_args+=(--options arrow:with_zstd=${ARROW_CONAN_WITH_ZSTD})
fi

version=$(grep '^set(ARROW_VERSION ' ${ARROW_HOME}/cpp/CMakeLists.txt | \
            grep -E -o '([0-9.]*)')

rm -rf ~/.conan/data/arrow/
rm -rf ${build_dir}/conan || sudo rm -rf ${build_dir}/conan
mkdir -p ${build_dir}/conan || sudo mkdir -p ${build_dir}/conan
if [ -w ${build_dir} ]; then
  cp -a ${source_dir}/ci/conan/* ${build_dir}/conan/
else
  sudo cp -a ${source_dir}/ci/conan/* ${build_dir}/conan/
  sudo chown -R $(id -u):$(id -g) ${build_dir}/conan/
fi
cd ${build_dir}/conan/all
conan create . arrow/${version}@ "${conan_args[@]}" "$@"
