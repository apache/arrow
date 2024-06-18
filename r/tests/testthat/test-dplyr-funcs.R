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

# Skip these tests on CRAN due to build times > 10 mins
skip_on_cran()

test_that("register_binding()/unregister_binding() works", {
  fun1 <- function() NULL
  fun2 <- function() "Hello"

  expect_null(register_binding("some.pkg::some_fun", fun1))
  expect_identical(.cache$functions$some_fun, fun1)
  expect_identical(.cache$functions$`some.pkg::some_fun`, fun1)

  expect_identical(unregister_binding("some.pkg::some_fun"), fun1)
  expect_false("some.pkg::some_fun" %in% names(.cache$functions))
  expect_false("some_fun" %in% names(.cache$functions))

  expect_null(register_binding("somePkg::some_fun", fun1))
  expect_identical(.cache$functions$some_fun, fun1)

  expect_warning(
    register_binding("some.pkg2::some_fun", fun2),
    "A \"some_fun\" binding already exists in the registry and will be overwritten."
  )

  # No warning when an identical function is re-registered
  expect_silent(register_binding("some.pkg2::some_fun", fun2))
})
