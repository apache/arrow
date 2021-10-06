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

skip_if(getRversion() <= "3.5.0")

is_altrep <- function(x) {
  !is.null(.Internal(altrep_class(x)))
}

test_that("altrep vectors from int32 and dbl arrays with no nulls", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  v_int <- Array$create(1:1000)
  v_dbl <- Array$create(as.numeric(1:1000))
  c_int <- ChunkedArray$create(1:1000)
  c_dbl <- ChunkedArray$create(as.numeric(1:1000))

  expect_true(is_altrep(as.vector(v_int)))
  expect_true(is_altrep(as.vector(v_int$Slice(1))))
  expect_true(is_altrep(as.vector(v_dbl)))
  expect_true(is_altrep(as.vector(v_dbl$Slice(1))))

  expect_equal(c_int$num_chunks, 1L)
  expect_true(is_altrep(as.vector(c_int)))
  expect_true(is_altrep(as.vector(c_int$Slice(1))))

  expect_equal(c_dbl$num_chunks, 1L)
  expect_true(is_altrep(as.vector(c_dbl)))
  expect_true(is_altrep(as.vector(c_dbl$Slice(1))))

  withr::local_options(list(arrow.use_altrep = NULL))
  expect_true(is_altrep(as.vector(v_int)))
  expect_true(is_altrep(as.vector(v_int$Slice(1))))
  expect_true(is_altrep(as.vector(v_dbl)))
  expect_true(is_altrep(as.vector(v_dbl$Slice(1))))

  withr::local_options(list(arrow.use_altrep = FALSE))
  expect_false(is_altrep(as.vector(v_int)))
  expect_false(is_altrep(as.vector(v_int$Slice(1))))
  expect_false(is_altrep(as.vector(v_dbl)))
  expect_false(is_altrep(as.vector(v_dbl$Slice(1))))
})

test_that("altrep vectors from int32 and dbl arrays with nulls", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  v_int <- Array$create(c(1L, NA, 3L))
  v_dbl <- Array$create(c(1, NA, 3))
  c_int <- ChunkedArray$create(c(1L, NA, 3L))
  c_dbl <- ChunkedArray$create(c(1, NA, 3))

  expect_true(is_altrep(as.vector(v_int)))
  expect_true(is_altrep(as.vector(v_int$Slice(1))))
  expect_true(is_altrep(as.vector(v_dbl)))
  expect_true(is_altrep(as.vector(v_dbl$Slice(1))))
  expect_true(is_altrep(as.vector(c_int)))
  expect_true(is_altrep(as.vector(c_int$Slice(1))))
  expect_true(is_altrep(as.vector(c_dbl)))
  expect_true(is_altrep(as.vector(c_dbl$Slice(1))))

  expect_true(is_altrep(as.vector(v_int$Slice(2))))
  expect_true(is_altrep(as.vector(v_dbl$Slice(2))))
  expect_true(is_altrep(as.vector(c_int$Slice(2))))
  expect_true(is_altrep(as.vector(c_dbl$Slice(2))))

  # chunked array with 2 chunks cannot be altrep
  c_int <- ChunkedArray$create(0L, c(1L, NA, 3L))
  c_dbl <- ChunkedArray$create(0, c(1, NA, 3))
  expect_equal(c_int$num_chunks, 2L)
  expect_equal(c_dbl$num_chunks, 2L)

  expect_false(is_altrep(as.vector(c_int)))
  expect_false(is_altrep(as.vector(c_dbl)))
  expect_true(is_altrep(as.vector(c_int$Slice(3))))
  expect_true(is_altrep(as.vector(c_dbl$Slice(3))))
})

test_that("empty vectors are not altrep", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  v_int <- Array$create(integer())
  v_dbl <- Array$create(numeric())

  expect_false(is_altrep(as.vector(v_int)))
  expect_false(is_altrep(as.vector(v_dbl)))
})

test_that("as.data.frame(<Table>, <RecordBatch>) can create altrep vectors", {
  withr::local_options(list(arrow.use_altrep = TRUE))

  table <- Table$create(int = c(1L, 2L, 3L), dbl = c(1, 2, 3), str = c("un", "deux", "trois"))
  df_table <- as.data.frame(table)
  expect_true(is_altrep(df_table$int))
  expect_true(is_altrep(df_table$dbl))
  expect_true(is_altrep(df_table$str))

  batch <- RecordBatch$create(int = c(1L, 2L, 3L), dbl = c(1, 2, 3), str = c("un", "deux", "trois"))
  df_batch <- as.data.frame(batch)
  expect_true(is_altrep(df_batch$int))
  expect_true(is_altrep(df_batch$dbl))
  expect_true(is_altrep(df_batch$str))
})

expect_altrep_rountrip <- function(x, fn, ...) {
  alt <- Array$create(x)$as_vector()

  expect_true(is_altrep(alt))
  expect_identical(fn(x, ...), fn(alt, ...))
  expect_true(is_altrep(alt))
}

test_that("altrep min/max/sum identical to R versions for double", {
  x <- c(1, 2, 3)
  expect_altrep_rountrip(x, min, na.rm = TRUE)
  expect_altrep_rountrip(x, max, na.rm = TRUE)
  expect_altrep_rountrip(x, sum, na.rm = TRUE)

  expect_altrep_rountrip(x, min)
  expect_altrep_rountrip(x, max)
  expect_altrep_rountrip(x, sum)

  x <- c(1, 2, NA_real_)
  expect_altrep_rountrip(x, min, na.rm = TRUE)
  expect_altrep_rountrip(x, max, na.rm = TRUE)
  expect_altrep_rountrip(x, sum, na.rm = TRUE)

  expect_altrep_rountrip(x, min)
  expect_altrep_rountrip(x, max)
  expect_altrep_rountrip(x, sum)

  x <- rep(NA_real_, 3)
  expect_warning(
    expect_altrep_rountrip(x, min, na.rm = TRUE),
    "no non-missing arguments to min"
  )
  expect_warning(
    expect_altrep_rountrip(x, max, na.rm = TRUE),
    "no non-missing arguments to max"
  )
  expect_altrep_rountrip(x, sum, na.rm = TRUE)

  expect_altrep_rountrip(x, min)
  expect_altrep_rountrip(x, max)
  expect_altrep_rountrip(x, sum)
})

test_that("altrep min/max/sum identical to R versions for int", {
  x <- c(1L, 2L, 3L)
  expect_altrep_rountrip(x, min, na.rm = TRUE)
  expect_altrep_rountrip(x, max, na.rm = TRUE)
  expect_altrep_rountrip(x, sum, na.rm = TRUE)

  expect_altrep_rountrip(x, min)
  expect_altrep_rountrip(x, max)
  expect_altrep_rountrip(x, sum)

  x <- c(1L, 2L, NA_integer_)
  expect_altrep_rountrip(x, min, na.rm = TRUE)
  expect_altrep_rountrip(x, max, na.rm = TRUE)
  expect_altrep_rountrip(x, sum, na.rm = TRUE)

  expect_altrep_rountrip(x, min)
  expect_altrep_rountrip(x, max)
  expect_altrep_rountrip(x, sum)

  x <- rep(NA_integer_, 3)
  expect_warning(
    expect_altrep_rountrip(x, min, na.rm = TRUE),
    "no non-missing arguments to min"
  )
  expect_warning(
    expect_altrep_rountrip(x, max, na.rm = TRUE),
    "no non-missing arguments to max"
  )
  expect_altrep_rountrip(x, sum, na.rm = TRUE)

  expect_altrep_rountrip(x, min)
  expect_altrep_rountrip(x, max)
  expect_altrep_rountrip(x, sum)

  # sum(x) is INT_MIN -> convert to double.
  x <- as.integer(c(-2^31 + 1L, -1L))
  expect_altrep_rountrip(x, sum)
})

test_that("altrep vectors handle serialization", {
  ints <- c(1L, 2L, NA_integer_)
  dbls <- c(1, 2, NA_real_)
  strs <- c("un", "deux", NA_character_)

  expect_identical(ints, unserialize(serialize(Array$create(ints)$as_vector(), NULL)))
  expect_identical(dbls, unserialize(serialize(Array$create(dbls)$as_vector(), NULL)))
  expect_identical(strs, unserialize(serialize(Array$create(strs)$as_vector(), NULL)))
  expect_identical(strs, unserialize(serialize(Array$create(strs, large_utf8())$as_vector(), NULL)))
})

test_that("altrep vectors handle coercion", {
  ints <- c(1L, 2L, NA_integer_)
  dbls <- c(1, 2, NA_real_)
  strs <- c("1", "2", NA_character_)

  expect_identical(ints, as.integer(Array$create(dbls)$as_vector()))
  expect_identical(ints, as.integer(Array$create(strs)$as_vector()))

  expect_identical(dbls, as.numeric(Array$create(ints)$as_vector()))
  expect_identical(dbls, as.numeric(Array$create(strs)$as_vector()))

  expect_identical(strs, as.character(Array$create(ints)$as_vector()))
  expect_identical(strs, as.character(Array$create(dbls)$as_vector()))
})

test_that("columns of struct types may be altrep", {
  st <- Array$create(data.frame(x = 1:10, y = runif(10)))
  df <- st$as_vector()

  expect_true(is_altrep(df$x))
  expect_true(is_altrep(df$y))
})
