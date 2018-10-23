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

context("arrow::Array")

test_that("Array", {
  x <- array(1:10, 1:10, 1:5)
  expect_equal(x$type(), int32())
  expect_equal(x$length(), 25L)
  expect_equal(x$as_vector(), c(1:10, 1:10, 1:5))

  y <- x$Slice(10)
  expect_equal(y$type(), int32())
  expect_equal(y$length(), 15L)
  expect_equal(y$as_vector(), c(1:10, 1:5))
  expect_true(x$RangeEquals(y, 10, 24, 0))

  z <- x$Slice(10, 5)
  expect_equal(z$type(), int32())
  expect_equal(z$length(), 5L)
  expect_equal(z$as_vector(), c(1:5))
  expect_true(x$RangeEquals(z, 10, 15, 0))

  x_dbl <- array(c(1,2,3), c(4,5,6))
  expect_equal(x_dbl$type(), float64())
  expect_equal(x_dbl$length(), 6L)
  expect_equal(x_dbl$as_vector(), as.numeric(1:6))

  y_dbl <- x_dbl$Slice(3)
  expect_equal(y_dbl$type(), float64())
  expect_equal(y_dbl$length(), 3L)
  expect_equal(y_dbl$offset(), 3L)
  expect_equal(y_dbl$as_vector(), as.numeric(4:6))

  z_dbl <- x_dbl$Slice(3, 2)
  expect_equal(z_dbl$type(), float64())
  expect_equal(z_dbl$length(), 2L)
  expect_equal(z_dbl$as_vector(), as.numeric(4:5))
})

test_that("Array supports NA", {
  x_int <- array(as.integer(c(1:10, NA)))
  x_dbl <- array(as.numeric(c(1:10, NA)))
  expect_true(x_int$IsValid(0L))
  expect_true(x_dbl$IsValid(0L))
  expect_true(x_int$IsNull(10L))
  expect_true(x_dbl$IsNull(10L))

  # this is not part of Array api
  expect_equal(Array__Mask(x_int), c(rep(TRUE, 10), FALSE))
  expect_equal(Array__Mask(x_dbl), c(rep(TRUE, 10), FALSE))
})

test_that("Array supports logical vectors (ARROW-3341)", {
  # with NA
  x <- sample(c(TRUE, FALSE, NA), 1000, replace = TRUE)
  arr_lgl <- array(x)
  expect_identical(x, arr_lgl$as_vector())

  # without NA
  x <- sample(c(TRUE, FALSE), 1000, replace = TRUE)
  arr_lgl <- array(x)
  expect_identical(x, arr_lgl$as_vector())
})

test_that("Array supports character vectors (ARROW-3339)", {
  # with NA
  x <- c("itsy", NA, "spider")
  arr_chr <- array(x)
  expect_equal(arr_chr$length(), 3L)
  expect_identical(arr_chr$as_vector(), x)
  expect_true(arr_chr$IsValid(0))
  expect_true(arr_chr$IsNull(1))
  expect_true(arr_chr$IsValid(2))

  sl <- arr_chr$Slice(1)
  expect_equal(sl$as_vector(), x[2:3])

  # without NA
  x <- c("itsy", "bitsy", "spider")
  arr_chr <- array(x)
  expect_equal(arr_chr$length(), 3L)
  expect_identical(arr_chr$as_vector(), x)
})

test_that("empty arrays are supported", {
  x <- character()
  expect_equal(array(x)$as_vector(), x)

  x <- integer()
  expect_equal(array(x)$as_vector(), x)

  x <- numeric()
  expect_equal(array(x)$as_vector(), x)

  x <- factor(character())
  expect_equal(array(x)$as_vector(), x)

  x <- logical()
  expect_equal(array(x)$as_vector(), x)
})

test_that("array with all nulls are supported", {
  nas <- c(NA, NA)

  x <- as.logical(nas)
  expect_equal(array(x)$as_vector(), x)

  x <- as.integer(nas)
  expect_equal(array(x)$as_vector(), x)

  x <- as.numeric(nas)
  expect_equal(array(x)$as_vector(), x)

  x <- as.character(nas)
  expect_equal(array(x)$as_vector(), x)

  x <- as.factor(nas)
  expect_equal(array(x)$as_vector(), x)
})

test_that("Array supports unordered factors (ARROW-3355)", {
  # without NA
  f <- factor(c("itsy", "bitsy", "spider", "spider"))
  arr_fac <- array(f)
  expect_equal(arr_fac$length(), 4L)
  expect_equal(arr_fac$type()$index_type(), int8())
  expect_identical(arr_fac$as_vector(), f)
  expect_true(arr_fac$IsValid(0))
  expect_true(arr_fac$IsValid(1))
  expect_true(arr_fac$IsValid(2))
  expect_true(arr_fac$IsValid(3))

  sl <- arr_fac$Slice(1)
  expect_equal(sl$length(), 3L)
  expect_equal(arr_fac$type()$index_type(), int8())
  expect_equal(sl$as_vector(), f[2:4])

  # with NA
  f <- factor(c("itsy", "bitsy", NA, "spider", "spider"))
  # TODO: rm the suppressWarnings when https://github.com/r-lib/vctrs/issues/109
  arr_fac <- suppressWarnings(array(f))
  expect_equal(arr_fac$length(), 5L)
  expect_equal(arr_fac$type()$index_type(), int8())
  expect_identical(arr_fac$as_vector(), f)
  expect_true(arr_fac$IsValid(0))
  expect_true(arr_fac$IsValid(1))
  expect_true(arr_fac$IsNull(2))
  expect_true(arr_fac$IsValid(3))
  expect_true(arr_fac$IsValid(4))

  sl <- arr_fac$Slice(1)
  expect_equal(sl$length(), 4L)
  expect_equal(arr_fac$type()$index_type(), int8())
  expect_equal(sl$as_vector(), f[2:5])
})

test_that("Array supports ordered factors (ARROW-3355)", {
  # without NA
  f <- ordered(c("itsy", "bitsy", "spider", "spider"))
  arr_fac <- array(f)
  expect_equal(arr_fac$length(), 4L)
  expect_equal(arr_fac$type()$index_type(), int8())
  expect_identical(arr_fac$as_vector(), f)
  expect_true(arr_fac$IsValid(0))
  expect_true(arr_fac$IsValid(1))
  expect_true(arr_fac$IsValid(2))
  expect_true(arr_fac$IsValid(3))

  sl <- arr_fac$Slice(1)
  expect_equal(sl$length(), 3L)
  expect_equal(arr_fac$type()$index_type(), int8())
  expect_equal(sl$as_vector(), f[2:4])

  # with NA
  f <- ordered(c("itsy", "bitsy", NA, "spider", "spider"))
  # TODO: rm the suppressWarnings when https://github.com/r-lib/vctrs/issues/109
  arr_fac <- suppressWarnings(array(f))
  expect_equal(arr_fac$length(), 5L)
  expect_equal(arr_fac$type()$index_type(), int8())
  expect_identical(arr_fac$as_vector(), f)
  expect_true(arr_fac$IsValid(0))
  expect_true(arr_fac$IsValid(1))
  expect_true(arr_fac$IsNull(2))
  expect_true(arr_fac$IsValid(3))
  expect_true(arr_fac$IsValid(4))

  sl <- arr_fac$Slice(1)
  expect_equal(sl$length(), 4L)
  expect_equal(arr_fac$type()$index_type(), int8())
  expect_equal(sl$as_vector(), f[2:5])
})

test_that("array supports Date (ARROW-3340)", {
  d <- Sys.Date() + 1:10
  a <- array(d)
  expect_equal(a$type(), date32())
  expect_equal(a$length(), 10L)
  expect_equal(a$as_vector(), d)

  d[5] <- NA
  a <- array(d)
  expect_equal(a$type(), date32())
  expect_equal(a$length(), 10L)
  expect_equal(a$as_vector(), d)
  expect_true(a$IsNull(4))

  d2 <- d + .5
  a <- array(d2)
  expect_equal(a$type(), date32())
  expect_equal(a$length(), 10L)
  expect_equal(a$as_vector(), d)
  expect_true(a$IsNull(4))
})

test_that("array supports POSIXct (ARROW-3340)", {
  times <- lubridate::ymd_hms("2018-10-07 19:04:05") + 1:10
  a <- array(times)
  expect_equal(a$type(), date64())
  expect_equal(a$length(), 10L)
  expect_equal(as.numeric(a$as_vector()), as.numeric(times))

  times[5] <- NA
  a <- array(times)
  expect_equal(a$type(), date32())
  expect_equal(a$length(), 10L)
  expect_equal(as.numeric(a$as_vector()), as.numeric(times))
  expect_true(a$IsNull(4))
})

test_that("array supports integer64", {
  x <- bit64::as.integer64(1:10)
  a <- array(x)
  expect_equal(a$type(), int64())
  expect_equal(a$length(), 10L)
  expect_equal(a$as_vector(), x)

  x[4] <- NA
  a <- array(x)
  expect_equal(a$type(), int64())
  expect_equal(a$length(), 10L)
  expect_equal(a$as_vector(), x)
  expect_true(a$IsNull(3L))
})
