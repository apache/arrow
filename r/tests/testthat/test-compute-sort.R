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

library(dplyr, warn.conflicts = FALSE)

# randomize order of rows in test data
tbl <- slice_sample(example_data_for_sorting, prop = 1L)

test_that("sort(Scalar) is identity function", {
  int <- Scalar$create(42L)
  expect_equal(sort(int), int)
  dbl <- Scalar$create(3.14)
  expect_equal(sort(dbl), dbl)
  chr <- Scalar$create("foo")
  expect_equal(sort(chr), chr)
})

test_that("Array$SortIndices()", {
  int <- tbl$int
  # Remove ties because they could give non-deterministic sort indices, and this
  # test compares sort indices. Other tests compare sorted values, which are
  # deterministic in the case of ties.
  int <- int[!duplicated(int)]
  expect_equal(
    Array$create(int)$SortIndices(),
    Array$create(order(int) - 1L, type = uint64())
  )
  # TODO(ARROW-14085): remove workaround once NA behavior is supported
  int <- na.omit(int)
  expect_equal(
    Array$create(int)$SortIndices(descending = TRUE),
    Array$create(rev(order(int)) - 1, type = uint64())
  )
})

test_that("ChunkedArray$SortIndices()", {
  int <- tbl$int
  # Remove ties because they could give non-deterministic sort indices, and this
  # test compares sort indices. Other tests compare sorted values, which are
  # deterministic in the case of ties.
  int <- int[!duplicated(int)]
  expect_equal(
    ChunkedArray$create(int[1:4], int[5:length(int)])$SortIndices(),
    Array$create(order(int) - 1L, type = uint64())
  )
  # TODO(ARROW-14085): remove workaround once NA behavior is supported
  int <- na.omit(int)
  expect_equal(
    ChunkedArray$create(int[1:4], int[5:length(int)])$SortIndices(descending = TRUE),
    Array$create(rev(order(int)) - 1, type = uint64())
  )
})

test_that("sort(vector), sort(Array), sort(ChunkedArray) give equivalent results on integers", {
  compare_expression(
    sort(.input),
    tbl$int
  )
  compare_expression(
    sort(.input, na.last = NA),
    tbl$int
  )
  compare_expression(
    sort(.input, na.last = TRUE),
    tbl$int
  )
  compare_expression(
    sort(.input, na.last = FALSE),
    tbl$int
  )
  compare_expression(
    sort(.input, decreasing = TRUE),
    tbl$int,
  )
  compare_expression(
    sort(.input, decreasing = TRUE, na.last = TRUE),
    tbl$int,
  )
  compare_expression(
    sort(.input, decreasing = TRUE, na.last = FALSE),
    tbl$int,
  )
})

test_that("sort(vector), sort(Array), sort(ChunkedArray) give equivalent results on strings", {
  compare_expression(
    sort(.input, decreasing = TRUE, na.last = FALSE),
    tbl$chr
  )
  compare_expression(
    sort(.input, decreasing = TRUE, na.last = FALSE),
    tbl$chr
  )
})

test_that("sort(vector), sort(Array), sort(ChunkedArray) give equivalent results on floats", {

  test_vec <- tbl$dbl
  # Arrow sorts NA and NaN differently, but it's not important, so eliminate here
  test_vec[is.nan(test_vec)] <- NA_real_

  compare_expression(
    sort(.input, decreasing = TRUE, na.last = TRUE),
    test_vec
  )
  compare_expression(
    sort(.input, decreasing = FALSE, na.last = TRUE),
    test_vec
  )
  compare_expression(
    sort(.input, decreasing = TRUE, na.last = NA),
    test_vec
  )
  compare_expression(
    sort(.input, decreasing = TRUE, na.last = FALSE),
    test_vec,
  )
  compare_expression(
    sort(.input, decreasing = FALSE, na.last = NA),
    test_vec
  )
  compare_expression(
    sort(.input, decreasing = FALSE, na.last = FALSE),
    test_vec,
  )
})

test_that("Table$SortIndices()", {
  x <- Table$create(tbl)
  expect_identical(
    as.vector(x$Take(x$SortIndices("chr"))$chr),
    sort(tbl$chr, na.last = TRUE)
  )
  expect_equal_data_frame(
    x$Take(x$SortIndices(c("int", "dbl"), c(FALSE, FALSE))),
    tbl %>% arrange(int, dbl)
  )
})

test_that("RecordBatch$SortIndices()", {
  x <- record_batch(tbl)
  expect_equal_data_frame(
    x$Take(x$SortIndices(c("chr", "int", "dbl"), TRUE)),
    tbl %>% arrange(desc(chr), desc(int), desc(dbl))
  )
})
