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

if [[ $# < 2 ]]; then
  echo "Usage: $0 <Arrow dir> <build dir> [ctest args ...]"
  exit 1
fi

arrow_dir=${1}; shift
build_dir=${1}/cpp; shift
source_dir=${arrow_dir}/cpp
binary_output_dir=${build_dir}/${ARROW_BUILD_TYPE:-debug}

export ARROW_TEST_DATA=${arrow_dir}/testing/data
export PARQUET_TEST_DATA=${source_dir}/submodules/parquet-testing/data
export LD_LIBRARY_PATH=${ARROW_HOME}/${CMAKE_INSTALL_LIBDIR:-lib}:${LD_LIBRARY_PATH}

# By default, aws-sdk tries to contact a non-existing local ip host
# to retrieve metadata. Disable this so that S3FileSystem tests run faster.
export AWS_EC2_METADATA_DISABLED=TRUE

# Enable memory debug checks.
export ARROW_DEBUG_MEMORY_POOL=trap

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
    # TODO: Enable these crashed tests.
    # https://issues.apache.org/jira/browse/ARROW-9072
    exclude_tests="gandiva-binary-test"
    exclude_tests="${exclude_tests}|gandiva-boolean-expr-test"
    exclude_tests="${exclude_tests}|gandiva-date-time-test"
    exclude_tests="${exclude_tests}|gandiva-decimal-single-test"
    exclude_tests="${exclude_tests}|gandiva-decimal-test"
    exclude_tests="${exclude_tests}|gandiva-filter-project-test"
    exclude_tests="${exclude_tests}|gandiva-filter-test"
    exclude_tests="${exclude_tests}|gandiva-hash-test"
    exclude_tests="${exclude_tests}|gandiva-if-expr-test"
    exclude_tests="${exclude_tests}|gandiva-in-expr-test"
    exclude_tests="${exclude_tests}|gandiva-internals-test"
    exclude_tests="${exclude_tests}|gandiva-literal-test"
    exclude_tests="${exclude_tests}|gandiva-null-validity-test"
    exclude_tests="${exclude_tests}|gandiva-precompiled-test"
    exclude_tests="${exclude_tests}|gandiva-projector-test"
    exclude_tests="${exclude_tests}|gandiva-utf8-test"
    ctest_options+=(--exclude-regex "${exclude_tests}")
    ;;
  *)
    n_jobs=${NPROC:-1}
    ;;
esac

pushd ${build_dir}

if [ -z "${PYTHON}" ] && ! which python > /dev/null 2>&1; then
  export PYTHON="${PYTHON:-python3}"
fi
ctest \
    --label-regex unittest \
    --output-on-failure \
    --parallel ${n_jobs} \
    --timeout ${ARROW_CTEST_TIMEOUT:-300} \
    "${ctest_options[@]}" \
    $@

if [ "${ARROW_BUILD_EXAMPLES}" == "ON" ]; then
    examples=$(find ${binary_output_dir} -executable -name "*example")
    if [ "${examples}" == "" ]; then
        echo "=================="
        echo "No examples found!"
        echo "=================="
        exit 1
    fi
    for ex in ${examples}
    do
        echo "=================="
        echo "Executing ${ex}"
        echo "=================="
        ${ex}
    done
fi

if [ "${ARROW_FUZZING}" == "ON" ]; then
    # Fuzzing regression tests
    ${binary_output_dir}/arrow-ipc-stream-fuzz ${ARROW_TEST_DATA}/arrow-ipc-stream/crash-*
    ${binary_output_dir}/arrow-ipc-stream-fuzz ${ARROW_TEST_DATA}/arrow-ipc-stream/*-testcase-*
    ${binary_output_dir}/arrow-ipc-file-fuzz ${ARROW_TEST_DATA}/arrow-ipc-file/*-testcase-*
    ${binary_output_dir}/arrow-ipc-tensor-stream-fuzz ${ARROW_TEST_DATA}/arrow-ipc-tensor-stream/*-testcase-*
    if [ "${ARROW_PARQUET}" == "ON" ]; then
      ${binary_output_dir}/parquet-arrow-fuzz ${ARROW_TEST_DATA}/parquet/fuzzing/*-testcase-*
    fi
fi

popd
