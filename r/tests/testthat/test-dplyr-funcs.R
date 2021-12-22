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

test_that("register_binding() works", {
  fake_registry <- new.env(parent = emptyenv())
  fun1 <- function() NULL

  expect_null(register_binding("some_fun", fun1, fake_registry))
  expect_identical(fake_registry$some_fun, fun1)

  expect_identical(register_binding("some_fun", NULL, fake_registry), fun1)
  expect_false("some_fun" %in% names(fake_registry))
  expect_silent(expect_null(register_binding("some_fun", NULL, fake_registry)))

  expect_null(register_binding("some_pkg::some_fun", fun1, fake_registry))
  expect_identical(fake_registry$some_fun, fun1)
})

test_that("register_binding_agg() works", {
  fake_registry <- new.env(parent = emptyenv())
  fun1 <- function() NULL

  expect_null(register_binding_agg("some_fun", fun1, fake_registry))
  expect_identical(fake_registry$some_fun, fun1)
})
