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

context("compute: sorting")

library(dplyr)

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
  # Need to remove NAs because ARROW-12063
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
  # Need to remove NAs because ARROW-12063
  int <- na.omit(int)
  expect_equal(
    ChunkedArray$create(int[1:4], int[5:length(int)])$SortIndices(descending = TRUE),
    Array$create(rev(order(int)) - 1, type = uint64())
  )
})

test_that("sort(vector), sort(Array), sort(ChunkedArray) give equivalent results on integers", {
  expect_vector_equal(
    sort(input),
    tbl$int
  )
  expect_vector_equal(
    sort(input, na.last = NA),
    tbl$int
  )
  expect_vector_equal(
    sort(input, na.last = TRUE),
    tbl$int
  )
  expect_vector_equal(
    sort(input, na.last = FALSE),
    tbl$int
  )
  expect_vector_equal(
    sort(input, decreasing = TRUE),
    tbl$int,
  )
  expect_vector_equal(
    sort(input, decreasing = TRUE, na.last = TRUE),
    tbl$int,
  )
  expect_vector_equal(
    sort(input, decreasing = TRUE, na.last = FALSE),
    tbl$int,
  )
})

test_that("sort(vector), sort(Array), sort(ChunkedArray) give equivalent results on strings", {
  expect_vector_equal(
    sort(input, decreasing = TRUE, na.last = FALSE),
    tbl$chr
  )
  expect_vector_equal(
    sort(input, decreasing = TRUE, na.last = FALSE),
    tbl$chr
  )
})

test_that("sort(vector), sort(Array), sort(ChunkedArray) give equivalent results on floats", {
  expect_vector_equal(
    sort(input, decreasing = TRUE, na.last = TRUE),
    tbl$dbl
  )
  expect_vector_equal(
    sort(input, decreasing = FALSE, na.last = TRUE),
    tbl$dbl
  )
  expect_vector_equal(
    sort(input, decreasing = TRUE, na.last = NA),
    tbl$dbl
  )
  expect_vector_equal(
    sort(input, decreasing = TRUE, na.last = FALSE),
    tbl$dbl,
  )
  expect_vector_equal(
    sort(input, decreasing = FALSE, na.last = NA),
    tbl$dbl
  )
  expect_vector_equal(
    sort(input, decreasing = FALSE, na.last = FALSE),
    tbl$dbl,
  )
})

test_that("Table$SortIndices()", {
  x <- Table$create(tbl)
  expect_identical(
    as.vector(x$Take(x$SortIndices("chr"))$chr),
    sort(tbl$chr, na.last = TRUE)
  )
  expect_identical(
    as.data.frame(x$Take(x$SortIndices(c("int", "dbl"), c(FALSE, FALSE)))),
    tbl %>% arrange(int, dbl)
  )
})

test_that("RecordBatch$SortIndices()", {
  x <- record_batch(tbl)
  expect_identical(
    as.data.frame(x$Take(x$SortIndices(c("chr", "int", "dbl"), TRUE))),
    tbl %>% arrange(desc(chr), desc(int), desc(dbl))
  )
})
