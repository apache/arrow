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

: ${R_BIN:=RDvalgrind}

source_dir=${1}/r

export CMAKE_BUILD_TYPE=RelWithDebInfo

${R_BIN} CMD INSTALL ${source_dir}
pushd ${source_dir}/tests

export TEST_R_WITH_ARROW=TRUE

# to generate suppression files run:
# ${R_BIN} --vanilla -d "valgrind --tool=memcheck --leak-check=full --track-origins=yes --gen-suppressions=all --log-file=memcheck.log" -f testtthat.supp
${R_BIN} --vanilla -d "valgrind --tool=memcheck --leak-check=full --track-origins=yes --suppressions=/${1}/ci/etc/valgrind-cran.supp" -f testthat.R |& tee testthat.out

# valgrind --error-exitcode=1 should return an erroring exit code that we can catch,
# but R eats that and returns 0, so we need to look at the output and make sure that
# we have 0 errors instead.
if [ $(grep -c "ERROR SUMMARY: 0 errors" testthat.out) != 1 ]; then
  cat testthat.out
  echo "Found Valgrind errors"
  exit 1
fi

# We might also considering using the greps that LibthGBM uses:
# https://github.com/microsoft/LightGBM/blob/fa6d356555f9ef888acf5f5e259dca958ca24f6d/.ci/test_r_package_valgrind.sh#L20-L85

popd
