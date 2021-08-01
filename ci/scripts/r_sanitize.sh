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

: ${R_BIN:=RDsan}

source_dir=${1}/r

pushd ${source_dir}

# Unity builds were causing the CI job to run out of memory
export CMAKE_UNITY_BUILD=OFF
# Make installation verbose so that the CI job doesn't time out due to silence
export ARROW_R_DEV=TRUE
${R_BIN} CMD INSTALL .
# But unset the env var so that it doesn't cause us to run extra dev tests
unset ARROW_R_DEV

export TEST_R_WITH_ARROW=TRUE
export UBSAN_OPTIONS="print_stacktrace=1,suppressions=/arrow/r/tools/ubsan.supp"

pushd tests
${R_BIN} < testthat.R > testthat.out 2>&1 || { cat testthat.out; exit 1; }
popd
${R_BIN} -e 'library(arrow); testthat::test_examples(".")' >> testthat.out 2>&1 || { cat testthat.out; exit 1; }

cat testthat.out
if grep -q "runtime error" testthat.out; then
  exit 1
fi
popd
