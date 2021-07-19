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

# cpp building dependencies
apt install -y cmake

# system dependencies needed for arrow's reverse dependencies
apt install -y libxml2-dev \
  libfontconfig1-dev \
  libcairo2-dev \
  libglpk-dev \
  libmysqlclient-dev \
  unixodbc-dev \
  libpq-dev \
  coinor-libsymphony-dev \
  coinor-libcgl-dev \
  coinor-symphony \
  libzmq3-dev \
  libudunits2-dev \
  libgdal-dev \
  libgeos-dev \
  libproj-dev

pushd ${source_dir}

printenv

: ${TEST_R_WITH_ARROW:=TRUE}
export TEST_R_WITH_ARROW=$TEST_R_WITH_ARROW

# By default, aws-sdk tries to contact a non-existing local ip host
# to retrieve metadata. Disable this so that S3FileSystem tests run faster.
export AWS_EC2_METADATA_DISABLED=TRUE

# Set crancache dir so we can cache it
export CRANCACHE_DIR="/arrow/.crancache"

SCRIPT="
    # We can't use RSPM binaries because we need source packages
    options('repos' = c(CRAN = 'https://packagemanager.rstudio.com/all/latest'))
    remotes::install_github('r-lib/revdepcheck')

    # zoo is needed by RcisTarget tests, though only listed in enhances so not installed by revdepcheck
    install.packages('zoo')

    # actually run revdepcheck
    revdepcheck::revdep_check(
    quiet = FALSE,
    timeout = as.difftime(120, units = 'mins'),
    num_workers = 1,
    env = c(
        ARROW_R_DEV = '$ARROW_R_DEV',
        LIBARROW_DOWNLOAD = TRUE,
        LIBARROW_MINIMAL = FALSE,
        revdepcheck::revdep_env_vars()
    ))
    revdepcheck::revdep_report(all = TRUE)

    # Go through the summary and fail if any of the statuses include -
    summary <- revdepcheck::revdep_summary()
    failed <- lapply(summary, function(check) grepl('-', check[['status']]))

    if (any(unlist(failed))) {
      quit(status = 1)
    }
    "

echo "$SCRIPT" | ${R_BIN} --no-save

popd
