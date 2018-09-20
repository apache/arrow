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

context("test-read-write")

test_that("arrow::table round trip", {
  tbl <- tibble(int = 1:10, dbl = as.numeric(1:10), raw = as.raw(1:10))

  tab <- arrow::table(tbl)
  expect_equal(tab$num_columns(), 3L)
  expect_equal(tab$num_rows(), 10L)

  expect_equal(tab$column(0)$length(), 10L)
  expect_equal(tab$column(1)$length(), 10L)
  expect_equal(tab$column(2)$length(), 10L)

  expect_equal(tab$column(0)$null_count(), 0L)
  expect_equal(tab$column(1)$null_count(), 0L)
  expect_equal(tab$column(2)$null_count(), 0L)

  expect_equal(tab$column(0)$type(), int32())
  expect_equal(tab$column(1)$type(), float64())
  expect_equal(tab$column(2)$type(), int8())

  tf <- tempfile(); on.exit(unlink(tf))
  write_arrow(tbl, path = tf)

  res <- read_arrow(tf)
  expect_identical(tbl, res)
})

test_that("arrow::table round trip handles NA in integer and numeric", {
  tbl <- tibble(int = c(NA, 2:10), dbl = as.numeric(c(1:5, NA, 7:9, NA)), raw = as.raw(1:10))

  tab <- arrow::table(tbl)
  expect_equal(tab$num_columns(), 3L)
  expect_equal(tab$num_rows(), 10L)

  expect_equal(tab$column(0)$length(), 10L)
  expect_equal(tab$column(1)$length(), 10L)
  expect_equal(tab$column(2)$length(), 10L)

  expect_equal(tab$column(0)$null_count(), 1L)
  expect_equal(tab$column(1)$null_count(), 2L)
  expect_equal(tab$column(2)$null_count(), 0L)

  expect_equal(tab$column(0)$type(), int32())
  expect_equal(tab$column(1)$type(), float64())
  expect_equal(tab$column(2)$type(), int8())

  tf <- tempfile(); on.exit(unlink(tf))
  write_arrow(tbl, path = tf)

  res <- read_arrow(tf)
  expect_identical(tbl, res)
  expect_true(is.na(res$int[1]))
  expect_true(is.na(res$dbl[6]))
  expect_true(is.na(res$dbl[10]))
})

