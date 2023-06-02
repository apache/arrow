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

test_that("check_versions", {
  expect_output(
    check_versions("10.0.0", "10.0.0"),
    "**** C++ and R library versions match: 10.0.0",
    fixed = TRUE
  )
  expect_output(
    expect_error(
      check_versions("10.0.0", "10.0.0-SNAPSHOT"),
      "version mismatch"
    ),
    "**** Not using: C++ library version (10.0.0-SNAPSHOT) does not match R package (10.0.0)",
    fixed = TRUE
  )
  expect_output(
    expect_error(
      check_versions("10.0.0.9000", "10.0.0-SNAPSHOT"),
      "version mismatch"
    ),
    "**** Not using: C++ library version (10.0.0-SNAPSHOT) does not match R package (10.0.0.9000)",
    fixed = TRUE
  )
  expect_output(
    expect_error(
      check_versions("10.0.0.9000", "10.0.0"),
      "version mismatch"
    ),
    "**** Not using: C++ library version (10.0.0) does not match R package (10.0.0.9000)",
    fixed = TRUE
  )
  expect_output(
    check_versions("10.0.0.9000", "11.0.0-SNAPSHOT"),
    "*** > Packages are both on development versions (11.0.0-SNAPSHOT, 10.0.0.9000)\n",
    fixed = TRUE
  )
})
