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


test_that("arrow_base_scalar_function() works", {
  # check in/out type as schema/data type
  fun <- arrow_base_scalar_function(schema(.y = int32()), int64(), function(x, y) y[[1]])
  expect_equal(attr(fun, "in_type")[[1]], schema(.y = int32()))
  expect_equal(attr(fun, "out_type")[[1]](), int64())

  # check in/out type as data type/data type
  fun <- arrow_base_scalar_function(int32(), int64(), function(x, y) y[[1]])
  expect_equal(attr(fun, "in_type")[[1]][[1]], field("", int32()))
  expect_equal(attr(fun, "out_type")[[1]](), int64())

  # check in/out type as field/data type
  fun <- arrow_base_scalar_function(
    field("a_name", int32()),
    int64(),
    function(x, y) y[[1]]
  )
  expect_equal(attr(fun, "in_type")[[1]], schema(a_name = int32()))
  expect_equal(attr(fun, "out_type")[[1]](), int64())

  # check in/out type as lists
  fun <- arrow_base_scalar_function(
    list(int32(), int64()),
    list(int64(), int32()),
    function(x, y) y[[1]]
  )

  expect_equal(attr(fun, "in_type")[[1]][[1]], field("", int32()))
  expect_equal(attr(fun, "in_type")[[2]][[1]], field("", int64()))
  expect_equal(attr(fun, "out_type")[[1]](), int64())
  expect_equal(attr(fun, "out_type")[[2]](), int32())

  expect_snapshot_error(arrow_base_scalar_function(int32(), int32(), identity))
  expect_snapshot_error(arrow_base_scalar_function(int32(), int32(), NULL))
})

test_that("arrow_scalar_function() returns a base scalar function", {
  base_fun <- arrow_scalar_function(
    list(float64(), float64()),
    float64(),
    function(x, y) {
      x + y
    }
  )

  expect_s3_class(base_fun, "arrow_base_scalar_function")
  expect_equal(
    base_fun(list(), list(Scalar$create(2), Array$create(3))),
    Array$create(5)
  )
})

test_that("register_scalar_function() adds a compute function to the registry", {
  skip_if_not_available("dataset")

  fun <- arrow_base_scalar_function(
    int32(), int64(),
    function(context, args) {
      args[[1]] + 1L
    }
  )

  register_scalar_function("my_test_scalar_function", fun)

  expect_true("my_test_scalar_function" %in% names(arrow:::.cache$functions))
  expect_true("my_test_scalar_function" %in% list_compute_functions())

  expect_equal(
    call_function("my_test_scalar_function", Array$create(1L, int32())),
    Array$create(2L, int64())
  )

  expect_equal(
    call_function("my_test_scalar_function", Scalar$create(1L, int32())),
    Scalar$create(2L, int64())
  )

  expect_identical(
    record_batch(a = 1L) %>%
      dplyr::mutate(b = my_test_scalar_function(a)) %>%
      dplyr::collect(),
    tibble::tibble(a = 1L, b = 2L)
  )
})
