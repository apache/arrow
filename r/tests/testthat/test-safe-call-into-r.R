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

# Note that TestSafeCallIntoR is defined in safe-call-into-r-impl.cpp

test_that("SafeCallIntoR works from the main R thread", {
  skip_on_cran()

  expect_identical(
    TestSafeCallIntoR(function() "string one!", opt = "on_main_thread"),
    "string one!"
  )

  expect_error(
    TestSafeCallIntoR(function() stop("an error!"), opt = "on_main_thread"),
    "an error!"
  )
})

test_that("SafeCallIntoR works within RunWithCapturedR", {
  skip_if_r_version("3.4.4")
  skip_on_cran()

  expect_identical(
    TestSafeCallIntoR(function() "string one!", opt = "async_with_executor"),
    "string one!"
  )

  expect_error(
    TestSafeCallIntoR(function() stop("an error!"), opt = "async_with_executor"),
    "an error!"
  )
})

test_that("SafeCallIntoR errors from the non-R thread", {
  skip_if_r_version("3.4.4")
  skip_on_cran()

  expect_error(
    TestSafeCallIntoR(function() "string one!", opt = "async_without_executor"),
    "Call to R from a non-R thread"
  )

  expect_error(
    TestSafeCallIntoR(function() stop("an error!"), opt = "async_without_executor"),
    "Call to R from a non-R thread"
  )
})
