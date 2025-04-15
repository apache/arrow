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
rhome=$(${R_BIN} RHOME)

pushd ${source_dir}

# Unity builds were causing the CI job to run out of memory
export CMAKE_UNITY_BUILD=OFF
# Make installation verbose so that the CI job doesn't time out due to silence
export ARROW_R_DEV=TRUE
# Get line numbers in sanitizer tracebacks
export CMAKE_BUILD_TYPE=RelWithDebInfo

ncores=$(${R_BIN} -s -e 'cat(parallel::detectCores())')
echo "MAKEFLAGS=-j${ncores}" >> ${rhome}/etc/Renviron.site

# build first so that any stray compiled files in r/src are ignored
${R_BIN} CMD build --no-build-vignettes --no-manual .

# But unset the env var so that it doesn't cause us to run extra dev tests
unset ARROW_R_DEV

# Set the testthat output to be verbose for easier debugging
export ARROW_R_VERBOSE_TEST=TRUE

# We prune dependencies for these, so we need to disable forcing suggests
export _R_CHECK_FORCE_SUGGESTS_=FALSE

export SUPPRESSION_FILE=$(readlink -f "tools/ubsan.supp")
export UBSAN_OPTIONS="print_stacktrace=1,suppressions=${SUPPRESSION_FILE}"
# From the old rhub image https://github.com/r-hub/rhub-linux-builders/blob/master/fedora-clang-devel-san/Dockerfile
export ASAN_OPTIONS="alloc_dealloc_mismatch=0:detect_leaks=0:detect_odr_violation=0"

${R_BIN} CMD check --no-manual --no-vignettes --no-build-vignettes arrow*.tar.gz

# Find sanitizer issues, print the file(s) they are part of, and fail the job
find . -type f -name "*Rout" -exec grep -l "runtime error\|SUMMARY: UndefinedBehaviorSanitizer" {} \; > sanitizer_errors.txt
if [ -s sanitizer_errors.txt ]; then
  echo "Sanitizer errors found in the following files:"
  cat sanitizer_errors.txt
  
  # Print the content of files with errors for debugging
  while read -r file; do
    echo "=============== $file ==============="
    cat "$file"
    echo "========================================="
  done < sanitizer_errors.txt
  
  exit 1
fi

popd
