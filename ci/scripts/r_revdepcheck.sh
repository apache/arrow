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

: ${R_BIN:=R}

source_dir=${1}/r

pushd ${source_dir}

printenv

# This is problematic?
# TODO: why?
# if [ "$ARROW_USE_PKG_CONFIG" != "false" ]; then
#   export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}
#   export R_LD_LIBRARY_PATH=${LD_LIBRARY_PATH}
# fi

export _R_CHECK_COMPILATION_FLAGS_KNOWN_=${ARROW_R_CXXFLAGS}
if [ "$ARROW_R_DEV" = "TRUE" ]; then
  # These are used in the Arrow C++ build and are not a problem
  export _R_CHECK_COMPILATION_FLAGS_KNOWN_="${_R_CHECK_COMPILATION_FLAGS_KNOWN_} -Wno-attributes -msse4.2"
  # Note that NOT_CRAN=true means (among other things) that optional dependencies are built
  export NOT_CRAN=true
fi
: ${TEST_R_WITH_ARROW:=TRUE}
export TEST_R_WITH_ARROW=$TEST_R_WITH_ARROW

# By default, aws-sdk tries to contact a non-existing local ip host
# to retrieve metadata. Disable this so that S3FileSystem tests run faster.
export AWS_EC2_METADATA_DISABLED=TRUE

SCRIPT="
    # We can't use RSPM binaries because we need source packages
    options('repos' = 'https://packagemanager.rstudio.com/all/latest')
    remotes::install_github('r-lib/revdepcheck')
    revdepcheck::revdep_check(
    quiet = FALSE,
    timeout = as.difftime(90, units = 'mins'),
    num_workers = 4,
    env = c(
        ARROW_R_DEV = '$ARROW_R_DEV',
        LIBARROW_DOWNLOAD = TRUE,
        LIBARROW_MINIMAL = FALSE,
        revdepcheck::revdep_env_vars()
    ))
    revdepcheck::revdep_report(all = TRUE)
    "

echo "$SCRIPT" | ${R_BIN} --no-save

popd
