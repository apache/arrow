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

tbl <- example_data_for_sorting

test_that("sort(Scalar) is identity function", {
  expect_identical(
    as.vector(sort(Scalar$create(42L))),
    42L
  )
  expect_identical(
    as.vector(sort(Scalar$create("foo"))),
    "foo"
  )
})

test_that("Array$SortIndices()", {
  expect_equal(
    Array$create(tbl$int)$SortIndices(),
    Array$create(0L:9L, type = uint64())
  )
  expect_equal(
    Array$create(rev(tbl$int))$SortIndices(descending = TRUE),
    Array$create(c(1L:9L, 0L), type = uint64())
  )
})

test_that("ChunkedArray$SortIndices()", {
  expect_equal(
    ChunkedArray$create(tbl$int[1:5], tbl$int[6:10])$SortIndices(),
    Array$create(0L:9L, type = uint64())
  )
  expect_equal(
    ChunkedArray$create(rev(tbl$int)[1:5], rev(tbl$int)[6:10])$SortIndices(descending = TRUE),
    Array$create(c(1L:9L, 0L), type = uint64())
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
  skip("is.na() evaluates to FALSE on Arrow NaN values (ARROW-12055)")
  expect_vector_equal(
    sort(input, decreasing = TRUE, na.last = FALSE),
    tbl$dbl
  )
  expect_vector_equal(
    sort(input, decreasing = TRUE, na.last = FALSE),
    tbl$dbl,
  )
})

test_that("Table$SortIndices()", {
  expect_identical(
    {
      x <- tbl %>% slice_sample(prop = 1L) %>% Table$create()
      x$Take(x$SortIndices("chr")) %>% pull(chr)
    },
    tbl$chr
  )
  expect_identical(
    {
      x <- tbl %>% slice_sample(prop = 1L) %>% Table$create()
      x$Take(x$SortIndices(c("int", "dbl"), c(FALSE, FALSE))) %>% collect()
    },
    tbl
  )
})

test_that("RecordBatch$SortIndices()", {
  expect_identical(
    {
      x <- tbl %>% slice_sample(prop = 1L) %>% record_batch()
      x$Take(x$SortIndices(c("chr", "int", "dbl"), TRUE)) %>% collect()
    },
    rbind(
      tbl %>% head(-1) %>% arrange(-row_number()),
      tbl %>% tail(1)
    )
  )
})
