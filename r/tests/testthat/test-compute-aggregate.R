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

test_that("list_compute_functions", {
  allfuncs <- list_compute_functions()
  expect_false(all(grepl("min", allfuncs)))
  justmins <- list_compute_functions("^min")
  expect_true(length(justmins) > 0)
  expect_true(all(grepl("min", justmins)))
  no_hash_funcs <- list_compute_functions("^hash")
  expect_true(length(no_hash_funcs) == 0)
})

test_that("sum.Array", {
  ints <- 1:5
  a <- Array$create(ints)
  expect_r6_class(sum(a), "Scalar")
  expect_identical(as.integer(sum(a)), sum(ints))

  floats <- c(1.3, 2.4, 3)
  f <- Array$create(floats)
  expect_identical(as.numeric(sum(f)), sum(floats))

  floats <- c(floats, NA)
  na <- Array$create(floats)
  if (!grepl("devel", R.version.string)) {
    # Valgrind on R-devel confuses NaN and NA_real_
    # https://r.789695.n4.nabble.com/Difference-in-NA-behavior-in-R-devel-running-under-valgrind-td4768731.html
    expect_identical(as.numeric(sum(na)), sum(floats))
  }
  expect_r6_class(sum(na, na.rm = TRUE), "Scalar")
  expect_identical(as.numeric(sum(na, na.rm = TRUE)), sum(floats, na.rm = TRUE))

  bools <- c(TRUE, NA, TRUE, FALSE)
  b <- Array$create(bools)
  expect_identical(as.integer(sum(b)), sum(bools))
  expect_identical(as.integer(sum(b, na.rm = TRUE)), sum(bools, na.rm = TRUE))
})

test_that("sum.ChunkedArray", {
  a <- ChunkedArray$create(1:4, c(1:4, NA), 1:5)
  expect_r6_class(sum(a), "Scalar")
  expect_true(is.na(as.vector(sum(a))))
  expect_identical(as.numeric(sum(a, na.rm = TRUE)), 35)
})

test_that("sum dots", {
  a1 <- Array$create(1:4)
  a2 <- ChunkedArray$create(1:4, c(1:4, NA), 1:5)
  expect_identical(as.numeric(sum(a1, a2, na.rm = TRUE)), 45)
})

test_that("sum.Scalar", {
  s <- Scalar$create(4)
  expect_identical(as.numeric(s), as.numeric(sum(s)))
})

test_that("mean.Array", {
  ints <- 1:4
  a <- Array$create(ints)
  expect_r6_class(mean(a), "Scalar")
  expect_identical(as.vector(mean(a)), mean(ints))

  floats <- c(1.3, 2.4, 3)
  f <- Array$create(floats)
  expect_identical(as.vector(mean(f)), mean(floats))

  floats <- c(floats, NA)
  na <- Array$create(floats)
  if (!grepl("devel", R.version.string)) {
    # Valgrind on R-devel confuses NaN and NA_real_
    # https://r.789695.n4.nabble.com/Difference-in-NA-behavior-in-R-devel-running-under-valgrind-td4768731.html
    expect_identical(as.vector(mean(na)), mean(floats))
  }
  expect_r6_class(mean(na, na.rm = TRUE), "Scalar")
  expect_identical(as.vector(mean(na, na.rm = TRUE)), mean(floats, na.rm = TRUE))

  bools <- c(TRUE, NA, TRUE, FALSE)
  b <- Array$create(bools)
  expect_identical(as.vector(mean(b)), mean(bools))
  expect_identical(as.integer(sum(b, na.rm = TRUE)), sum(bools, na.rm = TRUE))
})

test_that("mean.ChunkedArray", {
  a <- ChunkedArray$create(1:4, c(1:4, NA), 1:5)
  expect_r6_class(mean(a), "Scalar")
  expect_true(is.na(as.vector(mean(a))))
  expect_identical(as.vector(mean(a, na.rm = TRUE)), 35 / 13)
})

test_that("mean.Scalar", {
  s <- Scalar$create(4)
  expect_equal(s, mean(s))
})

test_that("Bad input handling of call_function", {
  expect_error(
    call_function("sum", 2, 3),
    'Argument 1 is of class numeric but it must be one of "Array", "ChunkedArray", "RecordBatch", "Table", or "Scalar"'
  )
})

test_that("min.Array", {
  ints <- 1:4
  a <- Array$create(ints)
  expect_r6_class(min(a), "Scalar")
  expect_identical(as.vector(min(a)), min(ints))

  floats <- c(1.3, 3, 2.4)
  f <- Array$create(floats)
  expect_identical(as.vector(min(f)), min(floats))

  floats <- c(floats, NA)
  na <- Array$create(floats)
  expect_identical(as.vector(min(na)), min(floats))
  expect_r6_class(min(na, na.rm = TRUE), "Scalar")
  expect_identical(as.vector(min(na, na.rm = TRUE)), min(floats, na.rm = TRUE))

  bools <- c(TRUE, TRUE, FALSE)
  b <- Array$create(bools)
  # R is inconsistent here: typeof(min(NA)) == "integer", not "logical"
  expect_identical(as.vector(min(b)), as.logical(min(bools)))
})

test_that("max.Array", {
  ints <- 1:4
  a <- Array$create(ints)
  expect_r6_class(max(a), "Scalar")
  expect_identical(as.vector(max(a)), max(ints))

  floats <- c(1.3, 3, 2.4)
  f <- Array$create(floats)
  expect_identical(as.vector(max(f)), max(floats))

  floats <- c(floats, NA)
  na <- Array$create(floats)
  expect_identical(as.vector(max(na)), max(floats))
  expect_r6_class(max(na, na.rm = TRUE), "Scalar")
  expect_identical(as.vector(max(na, na.rm = TRUE)), max(floats, na.rm = TRUE))

  bools <- c(TRUE, TRUE, FALSE)
  b <- Array$create(bools)
  # R is inconsistent here: typeof(max(NA)) == "integer", not "logical"
  expect_identical(as.vector(max(b)), as.logical(max(bools)))
})

test_that("min.ChunkedArray", {
  ints <- 1:4
  a <- ChunkedArray$create(ints)
  expect_r6_class(min(a), "Scalar")
  expect_identical(as.vector(min(a)), min(ints))

  floats <- c(1.3, 3, 2.4)
  f <- ChunkedArray$create(floats)
  expect_identical(as.vector(min(f)), min(floats))

  floats <- c(floats, NA)
  na <- ChunkedArray$create(floats)
  expect_identical(as.vector(min(na)), min(floats))
  expect_r6_class(min(na, na.rm = TRUE), "Scalar")
  expect_identical(as.vector(min(na, na.rm = TRUE)), min(floats, na.rm = TRUE))

  bools <- c(TRUE, TRUE, FALSE)
  b <- ChunkedArray$create(bools)
  # R is inconsistent here: typeof(min(NA)) == "integer", not "logical"
  expect_identical(as.vector(min(b)), as.logical(min(bools)))
})

test_that("max.ChunkedArray", {
  ints <- 1:4
  a <- ChunkedArray$create(ints)
  expect_r6_class(max(a), "Scalar")
  expect_identical(as.vector(max(a)), max(ints))

  floats <- c(1.3, 3, 2.4)
  f <- ChunkedArray$create(floats)
  expect_identical(as.vector(max(f)), max(floats))

  floats <- c(floats, NA)
  na <- ChunkedArray$create(floats)
  expect_identical(as.vector(max(na)), max(floats))
  expect_r6_class(max(na, na.rm = TRUE), "Scalar")
  expect_identical(as.vector(max(na, na.rm = TRUE)), max(floats, na.rm = TRUE))

  bools <- c(TRUE, TRUE, FALSE)
  b <- ChunkedArray$create(bools)
  # R is inconsistent here: typeof(max(NA)) == "integer", not "logical"
  expect_identical(as.vector(max(b)), as.logical(max(bools)))
})

test_that("Edge cases", {
  a <- Array$create(NA)
  for (type in c(int32(), float64(), bool())) {
    expect_as_vector(sum(a$cast(type), na.rm = TRUE), sum(NA, na.rm = TRUE))
    expect_as_vector(mean(a$cast(type), na.rm = TRUE), mean(NA, na.rm = TRUE))
    expect_as_vector(
      min(a$cast(type), na.rm = TRUE),
      # Suppress the base R warning about no non-missing arguments
      suppressWarnings(min(NA, na.rm = TRUE))
    )
    expect_as_vector(
      max(a$cast(type), na.rm = TRUE),
      suppressWarnings(max(NA, na.rm = TRUE))
    )
  }
})

test_that("quantile.Array and quantile.ChunkedArray", {
  a <- Array$create(c(0, 1, 2, 3))
  ca <- ChunkedArray$create(c(0, 1), c(2, 3))
  probs <- c(0.49, 0.51)
  for (ad in list(a, ca)) {
    for (type in c(int32(), uint64(), float64())) {
      expect_equal(
        quantile(ad$cast(type), probs = probs, interpolation = "linear"),
        Array$create(c(1.47, 1.53))
      )
      expect_equal(
        quantile(ad$cast(type), probs = probs, interpolation = "lower"),
        Array$create(c(1, 1))$cast(type)
      )
      expect_equal(
        quantile(ad$cast(type), probs = probs, interpolation = "higher"),
        Array$create(c(2, 2))$cast(type)
      )
      expect_equal(
        quantile(ad$cast(type), probs = probs, interpolation = "nearest"),
        Array$create(c(1, 2))$cast(type)
      )
      expect_equal(
        quantile(ad$cast(type), probs = probs, interpolation = "midpoint"),
        Array$create(c(1.5, 1.5))
      )
    }
  }
})

test_that("quantile and median NAs, edge cases, and exceptions", {
  expect_equal(
    quantile(Array$create(c(1, 2)), probs = c(0, 1)),
    Array$create(c(1, 2))
  )
  expect_error(
    quantile(Array$create(c(1, 2, NA))),
    "Missing values not allowed if 'na.rm' is FALSE"
  )
  expect_equal(
    quantile(Array$create(numeric(0))),
    Array$create(rep(NA_real_, 5))
  )
  expect_equal(
    quantile(Array$create(rep(NA_integer_, 3)), na.rm = TRUE),
    Array$create(rep(NA_real_, 5))
  )
  expect_equal(
    quantile(Scalar$create(0L)),
    Array$create(rep(0, 5))
  )
  expect_equal(
    median(Scalar$create(1L)),
    Scalar$create(1)
  )
  expect_error(
    quantile(Array$create(1:3), type = 9),
    "not supported"
  )
})

test_that("median passes ... args to quantile", {
  expect_equal(
    median(Array$create(c(1, 2)), interpolation = "higher"),
    Scalar$create(2)
  )
  expect_error(
    median(Array$create(c(1, 2)), probs = c(.25, .75))
  )
})

test_that("median.Array and median.ChunkedArray", {
  compare_expression(
    median(.input),
    1:4
  )
  compare_expression(
    median(.input),
    1:5
  )
  compare_expression(
    median(.input),
    numeric(0)
  )
  compare_expression(
    median(.input, na.rm = FALSE),
    c(1, 2, NA)
  )
  compare_expression(
    median(.input, na.rm = TRUE),
    c(1, 2, NA)
  )
  compare_expression(
    median(.input, na.rm = TRUE),
    NA_real_
  )
  compare_expression(
    median(.input, na.rm = FALSE),
    c(1, 2, NA)
  )
  compare_expression(
    median(.input, na.rm = TRUE),
    c(1, 2, NA)
  )
  compare_expression(
    median(.input, na.rm = TRUE),
    NA_real_
  )
})

test_that("unique.Array", {
  a <- Array$create(c(1, 4, 3, 1, 1, 3, 4))
  expect_equal(unique(a), Array$create(c(1, 4, 3)))
  ca <- ChunkedArray$create(a, a)
  expect_equal(unique(ca), Array$create(c(1, 4, 3)))
})

test_that("match_arrow", {
  a <- Array$create(c(1, 4, 3, 1, 1, 3, 4))
  tab <- c(4, 3, 2, 1)
  expect_equal(match_arrow(a, tab), Array$create(c(3L, 0L, 1L, 3L, 3L, 1L, 0L)))

  ca <- ChunkedArray$create(c(1, 4, 3, 1, 1, 3, 4))
  expect_equal(match_arrow(ca, tab), ChunkedArray$create(c(3L, 0L, 1L, 3L, 3L, 1L, 0L)))

  sc <- Scalar$create(3)
  expect_equal(match_arrow(sc, tab), Scalar$create(1L))

  vec <- c(1, 2)
  expect_equal(match_arrow(vec, tab), Array$create(c(3L, 2L)))
})

test_that("is_in", {
  a <- Array$create(c(9, 4, 3))
  tab <- c(4, 3, 2, 1)
  expect_equal(is_in(a, tab), Array$create(c(FALSE, TRUE, TRUE)))

  ca <- ChunkedArray$create(c(9, 4, 3))
  expect_equal(is_in(ca, tab), ChunkedArray$create(c(FALSE, TRUE, TRUE)))

  sc <- Scalar$create(3)
  expect_equal(is_in(sc, tab), Scalar$create(TRUE))

  vec <- c(1, 9)
  expect_equal(is_in(vec, tab), Array$create(c(TRUE, FALSE)))
})

test_that("value_counts", {
  a <- Array$create(c(1, 4, 3, 1, 1, 3, 4))
  result_df <- tibble::tibble(
    values = c(1, 4, 3),
    counts = c(3L, 2L, 2L)
  )
  result <- Array$create(
    result_df,
    type = struct(values = float64(), counts = int64())
  )
  expect_equal(value_counts(a), result)
  expect_equal_data_frame(value_counts(a), result_df)
  expect_identical(as.vector(value_counts(a)$counts), result_df$counts)
})

test_that("any.Array and any.ChunkedArray", {
  data <- c(1:10, NA, NA)

  compare_expression(any(.input > 5), data)
  compare_expression(any(.input > 5, na.rm = TRUE), data)
  compare_expression(any(.input < 1), data)
  compare_expression(any(.input < 1, na.rm = TRUE), data)

  data_logical <- c(TRUE, FALSE, TRUE, NA, FALSE)

  compare_expression(any(.input), data_logical)
  compare_expression(any(.input, na.rm = FALSE), data_logical)
  compare_expression(any(.input, na.rm = TRUE), data_logical)
})

test_that("all.Array and all.ChunkedArray", {
  data <- c(1:10, NA, NA)

  compare_expression(all(.input > 5), data)
  compare_expression(all(.input > 5, na.rm = TRUE), data)

  compare_expression(all(.input < 11), data)
  compare_expression(all(.input < 11, na.rm = TRUE), data)

  data_logical <- c(TRUE, TRUE, NA)

  compare_expression(all(.input), data_logical)
  compare_expression(all(.input, na.rm = TRUE), data_logical)
})

test_that("variance", {
  data <- c(-37, 267, 88, -120, 9, 101, -65, -23, NA)
  arr <- Array$create(data)
  chunked_arr <- ChunkedArray$create(data)

  expect_equal(call_function("variance", arr, options = list(ddof = 5)), Scalar$create(34596))
  expect_equal(call_function("variance", chunked_arr, options = list(ddof = 5)), Scalar$create(34596))
})

test_that("stddev", {
  data <- c(-37, 267, 88, -120, 9, 101, -65, -23, NA)
  arr <- Array$create(data)
  chunked_arr <- ChunkedArray$create(data)

  expect_equal(call_function("stddev", arr, options = list(ddof = 5)), Scalar$create(186))
  expect_equal(call_function("stddev", chunked_arr, options = list(ddof = 5)), Scalar$create(186))
})
