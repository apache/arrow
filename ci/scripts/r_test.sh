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

# Run the nixlibs.R test suite, which is not included in the installed package
${R_BIN} -e 'setwd("tools"); testthat::test_dir(".")'

# Before release, we always copy the relevant parts of the cpp source into the
# package. In some CI checks, we will use this version of the source:
# this is done by setting ARROW_SOURCE_HOME to something other than "/arrow"
# (which is where the arrow git checkout is found in docker and other CI jobs)
# In the other CI checks the files are synced but ignored.
make sync-cpp

if [ "$ARROW_R_FORCE_TESTS" = "true" ]; then
  export ARROW_R_DEV=TRUE
  export NOT_CRAN=true
  export ARROW_LARGE_MEMORY_TESTS=TRUE
fi

if [ "$ARROW_USE_PKG_CONFIG" != "false" ]; then
  export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}
  export R_LD_LIBRARY_PATH=${LD_LIBRARY_PATH}
fi
export _R_CHECK_COMPILATION_FLAGS_KNOWN_=${ARROW_R_CXXFLAGS}
if [ "$ARROW_R_DEV" = "TRUE" ]; then
  # These are sometimes used in the Arrow C++ build and are not a problem
  export _R_CHECK_COMPILATION_FLAGS_KNOWN_="${_R_CHECK_COMPILATION_FLAGS_KNOWN_} -Wno-attributes -msse4.2 -Wno-noexcept-type -Wno-subobject-linkage"
  if [ "$NOT_CRAN" = "" ]; then
    # Note that NOT_CRAN=true means (among other things) that optional dependencies are built
    # You can set NOT_CRAN=false for the CRAN build and then
    # ARROW_R_DEV=TRUE just adds verbosity
    export NOT_CRAN=true
  fi
fi

export _R_CHECK_CRAN_INCOMING_REMOTE_=FALSE
# --run-donttest was used in R < 4.0, this is used now
export _R_CHECK_DONTTEST_EXAMPLES_=TRUE
# Not all Suggested packages are needed for checking, so in case they aren't installed don't fail
export _R_CHECK_FORCE_SUGGESTS_=FALSE
export _R_CHECK_LIMIT_CORES_=FALSE
export _R_CHECK_TESTS_NLINES_=0

# By default, aws-sdk tries to contact a non-existing local ip host
# to retrieve metadata. Disable this so that S3FileSystem tests run faster.
export AWS_EC2_METADATA_DISABLED=TRUE

# Enable memory debug checks.
export ARROW_DEBUG_MEMORY_POOL=trap

# Hack so that texlive2020 doesn't pollute the home dir
export TEXMFCONFIG=/tmp/texmf-config
export TEXMFVAR=/tmp/texmf-var

# Make sure we aren't writing to the home dir (CRAN _hates_ this but there is no official check)
BEFORE=$(ls -alh ~/)

SCRIPT="as_cran <- !identical(tolower(Sys.getenv('NOT_CRAN')), 'true')
  if (as_cran) {
    args <- '--as-cran'
    build_args <- character()
  } else {
    args <- c('--no-manual', '--ignore-vignettes')
    build_args <- '--no-build-vignettes'
  }

  if (requireNamespace('reticulate', quietly = TRUE) && reticulate::py_module_available('pyarrow')) {
      message('Running flight demo server for tests.')
      pid_flight <- sys::exec_background(
          'python',
          c(
              '-c',
              paste0(
                  '__import__(\"sys\").path.append(\"./inst\"); ',
                  '__import__(\"demo_flight_server\").DemoFlightServer(port=8089).serve()'
              )
          )
      )
      on.exit(tools::pskill(pid_flight), add = TRUE)
  }

  run_donttest <- identical(tolower(Sys.getenv('_R_CHECK_DONTTEST_EXAMPLES_', 'true')), 'true')
  if (run_donttest) {
    args <- c(args, '--run-donttest')
  }

  install_args <- Sys.getenv('INSTALL_ARGS')
  if (nzchar(install_args)) {
    args <- c(args, paste0('--install-args=\"', install_args, '\"'))
  }

  rcmdcheck::rcmdcheck(build_args = build_args, args = args, error_on = 'warning', check_dir = 'check', timeout = 3600)"
echo "$SCRIPT" | ${R_BIN} --no-save

AFTER=$(ls -alh ~/)
if [ "$NOT_CRAN" != "true" ] && [ "$BEFORE" != "$AFTER" ]; then
  ls -alh ~/.cmake/packages
  exit 1
fi
popd
