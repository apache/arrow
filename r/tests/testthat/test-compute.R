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

test_that("list_compute_functions() works", {
  expect_type(list_compute_functions(), "character")
  expect_true(all(!grepl("^hash_", list_compute_functions())))
})


test_that("arrow_scalar_function() works", {
  # check in/out type as schema/data type
  fun <- arrow_scalar_function(schema(.y = int32()), int64(), function(x, y) y[[1]])
  expect_equal(attr(fun, "in_type")[[1]], schema(.y = int32()))
  expect_equal(attr(fun, "out_type")[[1]], int64())

  # check in/out type as data type/data type
  fun <- arrow_scalar_function(int32(), int64(), function(x, y) y[[1]])
  expect_equal(attr(fun, "in_type")[[1]], schema(.x = int32()))
  expect_equal(attr(fun, "out_type")[[1]], int64())

  # check in/out type as field/data type
  fun <- arrow_scalar_function(
    field("a_name", int32()),
    int64(),
    function(x, y) y[[1]]
  )
  expect_equal(attr(fun, "in_type")[[1]], schema(a_name = int32()))
  expect_equal(attr(fun, "out_type")[[1]], int64())

  # check in/out type as lists
  fun <- arrow_scalar_function(
    list(int32(), int64()),
    list(int64(), int32()),
    function(x, y) y[[1]]
  )

  expect_equal(attr(fun, "in_type")[[1]], schema(.x = int32()))
  expect_equal(attr(fun, "in_type")[[2]], schema(.x = int64()))
  expect_equal(attr(fun, "out_type")[[1]], int64())
  expect_equal(attr(fun, "out_type")[[1]], int64())

  expect_snapshot_error(arrow_scalar_function(int32(), int32(), identity))
  expect_snapshot_error(arrow_scalar_function(int32(), int32(), NULL))
})

test_that("register_scalar_function() creates a dplyr binding", {
  fun <- arrow_scalar_function(
    int32(),
    int64(),
    function(context, args) {
      args[[1]]
    }
  )

  register_scalar_function("my_test_scalar_function", fun)

  expect_true("my_test_scalar_function" %in% names(arrow:::.cache$functions))
  expect_true("my_test_scalar_function" %in% list_compute_functions())

  expect_equal(
    call_function("my_test_scalar_function", Array$create(1L, int32())),
    Array$create(1L, int64())
  )

  expect_equal(
    call_function("my_test_scalar_function", Scalar$create(1L, int32())),
    Scalar$create(1L, int64())
  )

  # fails because there's no event loop registered
  # record_batch(a = 1L) |> dplyr::mutate(b = arrow_my_test_scalar_function(a)) |> dplyr::collect()
})
