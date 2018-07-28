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

set -ex

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

pushd $CPP_BUILD_DIR

ctest -j2 --output-on-failure -L unittest

if [ "$ARROW_TRAVIS_CHECK_ABI" == "1" ]; then
  # Update this between major releases
  export REFERENCE_ABI_VERSION=9
  export ABI_VERSION=10
  wget "https://bintray.com/xhochy/arrow/download_file?file_path=abi-dumps%2FABI-${REFERENCE_ABI_VERSION}.dump.gz" -O ABI-${REFERENCE_ABI_VERSION}.dump.gz
  gunzip ABI-${REFERENCE_ABI_VERSION}.dump.gz
  abi-dumper -lver ${ABI_VERSION} debug/libarrow.so -o ABI-${ABI_VERSION}.dump

  function report_abi_conflicts() {
     echo "============= ABI problems ============="
     cat compat_reports/libarrow/${REFERENCE_ABI_VERSION}_to_${ABI_VERSION}/abi_affected.txt | c++filt
     echo "============= SRC problems ============="
     cat compat_reports/libarrow/${REFERENCE_ABI_VERSION}_to_${ABI_VERSION}/src_affected.txt | c++filt
     exit 1
  }

  abi-compliance-checker -l libarrow -d1 ABI-${REFERENCE_ABI_VERSION}.dump -d2 ABI-${ABI_VERSION}.dump -list-affected || report_abi_conflicts

  gzip -9 ABI-${ABI_VERSION}.dump
  export CURRENT_REVISION=`git rev-parse HEAD`
  mv ABI-${ABI_VERSION}.dump.gz ABI-${CURRENT_REVISION}.dump.gz
fi

popd

# Capture C++ coverage info (we wipe the build dir in travis_script_python.sh)
if [ "$ARROW_TRAVIS_COVERAGE" == "1" ]; then
    pushd $TRAVIS_BUILD_DIR
    lcov --quiet --directory . --capture --no-external --output-file $ARROW_CPP_COVERAGE_FILE
    popd
fi
