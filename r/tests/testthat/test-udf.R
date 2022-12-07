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
  fun <- arrow_scalar_function(
    function(context, x) x$cast(int64()),
    schema(x = int32()), int64()
  )
  expect_equal(fun$in_type[[1]], schema(x = int32()))
  expect_equal(fun$out_type[[1]](), int64())

  # check in/out type as data type/data type
  fun <- arrow_scalar_function(
    function(context, x) x$cast(int64()),
    int32(), int64()
  )
  expect_equal(fun$in_type[[1]][[1]], field("", int32()))
  expect_equal(fun$out_type[[1]](), int64())

  # check in/out type as field/data type
  fun <- arrow_scalar_function(
    function(context, a_name) x$cast(int64()),
    field("a_name", int32()),
    int64()
  )
  expect_equal(fun$in_type[[1]], schema(a_name = int32()))
  expect_equal(fun$out_type[[1]](), int64())

  # check in/out type as lists
  fun <- arrow_scalar_function(
    function(context, x) x,
    list(int32(), int64()),
    list(int64(), int32()),
    auto_convert = TRUE
  )

  expect_equal(fun$in_type[[1]][[1]], field("", int32()))
  expect_equal(fun$in_type[[2]][[1]], field("", int64()))
  expect_equal(fun$out_type[[1]](), int64())
  expect_equal(fun$out_type[[2]](), int32())

  expect_snapshot_error(arrow_scalar_function(NULL, int32(), int32()))
})

test_that("arrow_scalar_function() works with auto_convert = TRUE", {
  times_32_wrapper <- arrow_scalar_function(
    function(context, x) x * 32,
    float64(),
    float64(),
    auto_convert = TRUE
  )

  dummy_kernel_context <- list()

  expect_equal(
    times_32_wrapper$wrapper_fun(dummy_kernel_context, list(Scalar$create(2))),
    Array$create(2 * 32)
  )
})

test_that("register_scalar_function() adds a compute function to the registry", {
  skip_if_not(CanRunWithCapturedR())
  # TODO(ARROW-17178): User-defined function-friendly ExecPlan execution has
  # occasional valgrind errors
  skip_on_linux_devel()

  register_scalar_function(
    "times_32",
    function(context, x) x * 32.0,
    int32(), float64(),
    auto_convert = TRUE
  )
  on.exit(unregister_binding("times_32", update_cache = TRUE))

  expect_true("times_32" %in% names(asNamespace("arrow")$.cache$functions))
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

test_that("arrow_scalar_function() with bad return type errors", {
  skip_if_not(CanRunWithCapturedR())

  register_scalar_function(
    "times_32_bad_return_type_array",
    function(context, x) Array$create(x, int32()),
    int32(),
    float64()
  )
  on.exit(unregister_binding("times_32_bad_return_type_array", update_cache = TRUE))

  expect_error(
    call_function("times_32_bad_return_type_array", Array$create(1L)),
    "Expected return Array or Scalar with type 'double'"
  )

  register_scalar_function(
    "times_32_bad_return_type_scalar",
    function(context, x) Scalar$create(x, int32()),
    int32(),
    float64()
  )
  on.exit(unregister_binding("times_32_bad_return_type_scalar", update_cache = TRUE))

  expect_error(
    call_function("times_32_bad_return_type_scalar", Array$create(1L)),
    "Expected return Array or Scalar with type 'double'"
  )
})

test_that("register_scalar_function() can register multiple kernels", {
  skip_if_not(CanRunWithCapturedR())

  register_scalar_function(
    "times_32",
    function(context, x) x * 32L,
    in_type = list(int32(), int64(), float64()),
    out_type = function(in_types) in_types[[1]],
    auto_convert = TRUE
  )
  on.exit(unregister_binding("times_32", update_cache = TRUE))

  expect_equal(
    call_function("times_32", Scalar$create(1L, int32())),
    Scalar$create(32L, int32())
  )

  expect_equal(
    call_function("times_32", Scalar$create(1L, int64())),
    Scalar$create(32L, int64())
  )

  expect_equal(
    call_function("times_32", Scalar$create(1L, float64())),
    Scalar$create(32L, float64())
  )
})

test_that("register_scalar_function() errors for unsupported specifications", {
  expect_error(
    register_scalar_function(
      "no_kernels",
      function(...) NULL,
      list(),
      list()
    ),
    "Can't register user-defined scalar function with 0 kernels"
  )

  expect_error(
    register_scalar_function(
      "wrong_n_args",
      function(x) NULL,
      int32(),
      int32()
    ),
    "Expected `fun` to accept 2 argument\\(s\\)"
  )

  expect_error(
    register_scalar_function(
      "var_kernels",
      function(...) NULL,
      list(float64(), schema(x = float64(), y = float64())),
      float64()
    ),
    "Kernels for user-defined function must accept the same number of arguments"
  )
})

test_that("user-defined functions work during multi-threaded execution", {
  skip_if_not(CanRunWithCapturedR())
  skip_if_not_available("dataset")
  # Skip on linux devel because:
  # TODO(ARROW-17283): Snappy has a UBSan issue that is fixed in the dev version
  # TODO(ARROW-17178): User-defined function-friendly ExecPlan execution has
  # occasional valgrind errors
  skip_on_linux_devel()

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

  tf_dataset <- tempfile()
  tf_dest <- tempfile()
  on.exit(unlink(c(tf_dataset, tf_dest)))
  write_dataset(example_df, tf_dataset, partitioning = "part")

  register_scalar_function(
    "times_32",
    function(context, x) x * 32.0,
    int32(),
    float64(),
    auto_convert = TRUE
  )
  on.exit(unregister_binding("times_32", update_cache = TRUE))

  # check a regular collect()
  result <- open_dataset(tf_dataset) %>%
    dplyr::mutate(fun_result = times_32(value)) %>%
    dplyr::collect() %>%
    dplyr::arrange(row_num)

  expect_identical(result$fun_result, example_df$value * 32)

  # check a write_dataset()
  open_dataset(tf_dataset) %>%
    dplyr::mutate(fun_result = times_32(value)) %>%
    write_dataset(tf_dest)

  result2 <- dplyr::collect(open_dataset(tf_dest)) %>%
    dplyr::arrange(row_num) %>%
    dplyr::collect()

  expect_identical(result2$fun_result, example_df$value * 32)
})

test_that("nested exec plans can contain user-defined functions", {
  skip_if_not_available("dataset")
  skip_if_not(CanRunWithCapturedR())

  register_scalar_function(
    "times_32",
    function(context, x) x * 32.0,
    int32(),
    float64(),
    auto_convert = TRUE
  )
  on.exit(unregister_binding("times_32", update_cache = TRUE))

  stream_plan_with_udf <- function() {
    record_batch(a = 1:1000) %>%
      dplyr::mutate(b = times_32(a)) %>%
      as_record_batch_reader() %>%
      as_arrow_table()
  }

  collect_plan_with_head <- function() {
    record_batch(a = 1:1000) %>%
      dplyr::mutate(fun_result = times_32(a)) %>%
      head(11) %>%
      dplyr::collect()
  }

  expect_equal(
    stream_plan_with_udf(),
    record_batch(a = 1:1000) %>%
      dplyr::mutate(b = times_32(a)) %>%
      dplyr::collect(as_data_frame = FALSE)
  )

  result <- collect_plan_with_head()
  expect_equal(nrow(result), 11)
})

test_that("head() on exec plan containing user-defined functions", {
  skip("ARROW-18101")
  skip_if_not_available("dataset")
  skip_if_not(CanRunWithCapturedR())

  register_scalar_function(
    "times_32",
    function(context, x) x * 32.0,
    int32(),
    float64(),
    auto_convert = TRUE
  )
  on.exit(unregister_binding("times_32", update_cache = TRUE))

  result <- record_batch(a = 1:1000) %>%
    dplyr::mutate(b = times_32(a)) %>%
    as_record_batch_reader() %>%
    head(11) %>%
    dplyr::collect()

  expect_equal(nrow(result), 11)
})
