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


# Usage: run testthat::test_dir(".") inside of this directory

# Flag so that we just load the functions and don't evaluate them like we do
# when called from configure.R
TESTING <- TRUE

source("check-versions.R", local = TRUE)

test_that("check_versions without mismatch", {
  expect_output(
    check_versions("10.0.0", "10.0.0"),
    "**** C++ library version 10.0.0 is supported by R version 10.0.0",
    fixed = TRUE
  )
  expect_output(
    expect_error(
      check_versions("10.0.0", "10.0.0-SNAPSHOT"),
      "version mismatch"
    ),
    "**** Not using: C++ library version (10.0.0-SNAPSHOT): not supported by R package version 10.0.0",
    fixed = TRUE
  )
  expect_output(
    expect_error(
      check_versions("10.0.0.9000", "10.0.0-SNAPSHOT"),
      "version mismatch"
    ),
    "**** Not using: C++ library version (10.0.0-SNAPSHOT): not supported by R package version 10.0.0.9000",
    fixed = TRUE
  )
  expect_output(
    check_versions("10.0.0.3", "10.0.0"),
    "**** C++ library version 10.0.0 is supported by R version 10.0.0.3",
    fixed = TRUE
  )
  expect_output(
    check_versions("10.0.0.9000", "11.0.0-SNAPSHOT"),
    "*** > Packages are both on development versions (11.0.0-SNAPSHOT, 10.0.0.9000)\n",
    fixed = TRUE
  )
})

test_that("check_versions with mismatch", {
  withr::local_envvar(.new = c(ARROW_R_ALLOW_CPP_VERSION_MISMATCH = "false"))

  expect_false(
    release_version_supported("15.0.0", "13.0.0")
  )

  withr::local_envvar(.new = c(ARROW_R_ALLOW_CPP_VERSION_MISMATCH = "true"))

  expect_true(
    release_version_supported("15.0.0", "13.0.0")
  )

  expect_false(
    release_version_supported("15.0.0", "16.0.0")
  )

  expect_false(
    release_version_supported("15.0.0", "12.0.0")
  )
})
