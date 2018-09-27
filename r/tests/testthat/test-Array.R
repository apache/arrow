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

