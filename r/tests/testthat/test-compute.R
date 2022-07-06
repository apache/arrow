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


test_that("arrow_advanced_scalar_function() works", {
  # check in/out type as schema/data type
  fun <- arrow_advanced_scalar_function(
    schema(.y = int32()), int64(),
    function(kernel_context, args) args[[1]]
  )
  expect_equal(attr(fun, "in_type")[[1]], schema(.y = int32()))
  expect_equal(attr(fun, "out_type")[[1]](), int64())

  # check in/out type as data type/data type
  fun <- arrow_advanced_scalar_function(
    int32(), int64(),
    function(kernel_context, args) args[[1]]
  )
  expect_equal(attr(fun, "in_type")[[1]][[1]], field("", int32()))
  expect_equal(attr(fun, "out_type")[[1]](), int64())

  # check in/out type as field/data type
  fun <- arrow_advanced_scalar_function(
    field("a_name", int32()),
    int64(),
    function(kernel_context, args) args[[1]]
  )
  expect_equal(attr(fun, "in_type")[[1]], schema(a_name = int32()))
  expect_equal(attr(fun, "out_type")[[1]](), int64())

  # check in/out type as lists
  fun <- arrow_advanced_scalar_function(
    list(int32(), int64()),
    list(int64(), int32()),
    function(kernel_context, args) args[[1]]
  )

  expect_equal(attr(fun, "in_type")[[1]][[1]], field("", int32()))
  expect_equal(attr(fun, "in_type")[[2]][[1]], field("", int64()))
  expect_equal(attr(fun, "out_type")[[1]](), int64())
  expect_equal(attr(fun, "out_type")[[2]](), int32())

  expect_snapshot_error(arrow_advanced_scalar_function(int32(), int32(), identity))
  expect_snapshot_error(arrow_advanced_scalar_function(int32(), int32(), NULL))
})

test_that("arrow_scalar_function() returns a base scalar function", {
  base_fun <- arrow_scalar_function(
    list(float64(), float64()),
    float64(),
    function(x, y) {
      x + y
    }
  )

  expect_s3_class(base_fun, "arrow_advanced_scalar_function")
  expect_equal(
    base_fun(list(), list(Scalar$create(2), Array$create(3))),
    Array$create(5)
  )
})

test_that("register_scalar_function() adds a compute function to the registry", {
  skip_if_not_available("dataset")

  times_32 <- arrow_scalar_function(
    int32(), float64(),
    function(x) x * 32.0
  )

  register_scalar_function("times_32", times_32)

  expect_true("times_32" %in% names(arrow:::.cache$functions))
  expect_true("times_32" %in% list_compute_functions())

  expect_equal(
    call_function("times_32", Array$create(1L, int32())),
    Array$create(32L, float64())
  )

  expect_equal(
    call_function("times_32", Scalar$create(1L, int32())),
    Scalar$create(32L, float64())
  )

  expect_identical(
    record_batch(a = 1L) %>%
      dplyr::mutate(b = times_32(a)) %>%
      dplyr::collect(),
    tibble::tibble(a = 1L, b = 32.0)
  )
})

test_that("user-defined functions work during multi-threaded execution", {
  skip_if_not_available("dataset")

  n_rows <- 10000
  n_partitions <- 10
  example_df <- expand.grid(
    part = letters[seq_len(n_partitions)],
    value = seq_len(n_rows),
    stringsAsFactors = FALSE
  )

  # make sure values are different for each partition and
  example_df$row_num <- seq_len(nrow(example_df))
  example_df$value <- example_df$value + match(example_df$part, letters)

  tf <- tempfile()
  on.exit(unlink(tf))
  write_dataset(example_df, tf, partitioning = "part")

  times_32 <- arrow_scalar_function(
    int32(), float64(),
    function(x) x * 32.0
  )

  register_scalar_function("times_32", times_32)

  result <- open_dataset(tf) %>%
    dplyr::mutate(fun_result = times_32(value)) %>%
    dplyr::collect() %>%
    dplyr::arrange(row_num) %>%
    tibble::as_tibble()

  expect_identical(result$fun_result, example_df$value * 32)
})
