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
source_dir=${1}/cpp
build_dir=${2}/cpp
binary_output_dir=${build_dir}/${ARROW_BUILD_TYPE:-debug}

export ARROW_TEST_DATA=${arrow_dir}/testing/data
export PARQUET_TEST_DATA=${source_dir}/submodules/parquet-testing/data
export LD_LIBRARY_PATH=${ARROW_HOME}/${CMAKE_INSTALL_LIBDIR:-lib}:${LD_LIBRARY_PATH}

# By default, aws-sdk tries to contact a non-existing local ip host
# to retrieve metadata. Disable this so that S3FileSystem tests run faster.
export AWS_EC2_METADATA_DISABLED=TRUE

ctest_options=()
case "$(uname)" in
  Linux)
    n_jobs=$(nproc)
    ;;
  Darwin)
    n_jobs=$(sysctl -n hw.ncpu)
    ;;
  MINGW*)
    n_jobs=${NUMBER_OF_PROCESSORS:-1}
    ctest_options+=(--exclude-regex gandiva-internals-test)
    ctest_options+=(--exclude-regex gandiva-projector-test)
    ctest_options+=(--exclude-regex gandiva-utf8-test)
    if [ "${MSYSTEM}" = "MINGW32" ]; then
      ctest_options+=(--exclude-regex gandiva-binary-test)
      ctest_options+=(--exclude-regex gandiva-boolean-expr-test)
      ctest_options+=(--exclude-regex gandiva-date-time-test)
      ctest_options+=(--exclude-regex gandiva-decimal-single-test)
      ctest_options+=(--exclude-regex gandiva-decimal-test)
      ctest_options+=(--exclude-regex gandiva-filter-project-test)
      ctest_options+=(--exclude-regex gandiva-filter-test)
      ctest_options+=(--exclude-regex gandiva-hash-test)
      ctest_options+=(--exclude-regex gandiva-if-expr-test)
      ctest_options+=(--exclude-regex gandiva-in-expr-test)
      ctest_options+=(--exclude-regex gandiva-literal-test)
      ctest_options+=(--exclude-regex gandiva-null-validity-test)
    fi
    ;;
  *)
    n_jobs=${NPROC:-1}
    ;;
esac

pushd ${build_dir}

if ! which python > /dev/null 2>&1; then
  export PYTHON=python3
fi
ctest \
    --label-regex unittest \
    --output-on-failure \
    --parallel ${n_jobs} \
    --timeout 300 \
    "${ctest_options[@]}"

if [ "${ARROW_FUZZING}" == "ON" ]; then
    # Fuzzing regression tests
    ${binary_output_dir}/arrow-ipc-stream-fuzz ${ARROW_TEST_DATA}/arrow-ipc-stream/crash-*
    ${binary_output_dir}/arrow-ipc-stream-fuzz ${ARROW_TEST_DATA}/arrow-ipc-stream/*-testcase-*
    ${binary_output_dir}/arrow-ipc-file-fuzz ${ARROW_TEST_DATA}/arrow-ipc-file/*-testcase-*
    if [ "${ARROW_PARQUET}" == "ON" ]; then
      ${binary_output_dir}/parquet-arrow-fuzz ${ARROW_TEST_DATA}/parquet/fuzzing/*-testcase-*
    fi
fi

popd
