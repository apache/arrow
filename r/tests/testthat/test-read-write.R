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

context("read-write")

test_that("arrow::table round trip", {
  tbl <- tibble::tibble(
    int = 1:10,
    dbl = as.numeric(1:10),
    raw = as.raw(1:10)
  )

  tab <- arrow::table(tbl)
  expect_equal(tab$num_columns(), 3L)
  expect_equal(tab$num_rows(), 10L)

  # arrow::Column
  col_int <- tab$column(0)
  expect_equal(col_int$length(), 10L)
  expect_equal(col_int$null_count(), 0L)
  expect_equal(col_int$type(), int32())

  # arrow::ChunkedArray
  chunked_array_int <- col_int$data()
  expect_equal(chunked_array_int$length(), 10L)
  expect_equal(chunked_array_int$null_count(), 0L)
  expect_equal(chunked_array_int$as_vector(), tbl$int)

  # arrow::Array
  chunks_int <- chunked_array_int$chunks()
  expect_equal(length(chunks_int), chunked_array_int$num_chunks())
  for( i in seq_along(chunks_int)){
    expect_equal(chunked_array_int$chunk(i-1L), chunks_int[[i]])
  }

  # arrow::Column
  col_dbl <- tab$column(1)
  expect_equal(col_dbl$length(), 10L)
  expect_equal(col_dbl$null_count(), 0L)
  expect_equal(col_dbl$type(), float64())

  # arrow::ChunkedArray
  chunked_array_dbl <- col_dbl$data()
  expect_equal(chunked_array_dbl$length(), 10L)
  expect_equal(chunked_array_dbl$null_count(), 0L)
  expect_equal(chunked_array_dbl$as_vector(), tbl$dbl)

  # arrow::Array
  chunks_dbl <- chunked_array_dbl$chunks()
  expect_equal(length(chunks_dbl), chunked_array_dbl$num_chunks())
  for( i in seq_along(chunks_dbl)){
    expect_equal(chunked_array_dbl$chunk(i-1L), chunks_dbl[[i]])
  }

  # arrow::Colmumn
  col_raw <- tab$column(2)
  expect_equal(col_raw$length(), 10L)
  expect_equal(col_raw$null_count(), 0L)
  expect_equal(col_raw$type(), int8())

  # arrow::ChunkedArray
  chunked_array_raw <- col_raw$data()
  expect_equal(chunked_array_raw$length(), 10L)
  expect_equal(chunked_array_raw$null_count(), 0L)
  expect_equal(chunked_array_raw$as_vector(), tbl$raw)

  # arrow::Array
  chunks_raw <- chunked_array_raw$chunks()
  expect_equal(length(chunks_raw), chunked_array_raw$num_chunks())
  for( i in seq_along(chunks_raw)){
    expect_equal(chunked_array_raw$chunk(i-1L), chunks_raw[[i]])
  }
  tf <- local_tempfile()
  write_arrow(tbl, tf)

  res <- read_arrow(tf)
  expect_identical(tbl, res)
})

test_that("arrow::table round trip handles NA in integer and numeric", {
  tbl <- tibble::tibble(
    int = c(NA, 2:10),
    dbl = as.numeric(c(1:5, NA, 7:9, NA)),
    raw = as.raw(1:10)
  )

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

  tf <- local_tempfile()
  write_arrow(tbl, tf)

  res <- read_arrow(tf)
  expect_identical(tbl, res)
  expect_true(is.na(res$int[1]))
  expect_true(is.na(res$dbl[6]))
  expect_true(is.na(res$dbl[10]))
})

