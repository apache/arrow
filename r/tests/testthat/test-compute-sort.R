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

test_that("Array sort", {
  expect_equal(
    Array$create(tbl$int)$SortIndices(),
    Array$create(0L:9L, type = uint64())
  )
  expect_equal(
    Array$create(rev(tbl$int))$SortIndices(descending = TRUE),
    Array$create(c(1L:9L, 0L), type = uint64())
  )
  expect_equal(
    as.vector(sort(Array$create(tbl$int))),
    sort(tbl$int, na.last = TRUE)
  )
  expect_equal(
    as.vector(sort(Array$create(tbl$int), decreasing = TRUE)),
    sort(tbl$int, decreasing = TRUE, na.last = TRUE)
  )
  expect_error(
    sort(Array$create(tbl$int), decreasing = TRUE, na.last = NA),
    "na.last",
    fixed = TRUE
  )
})

test_that("ChunkedArray sort", {
  expect_equal(
    ChunkedArray$create(tbl$int[1:5], tbl$int[6:10])$SortIndices(),
    Array$create(0L:9L, type = uint64())
  )
  expect_equal(
    ChunkedArray$create(rev(tbl$int)[1:5], rev(tbl$int)[6:10])$SortIndices(descending = TRUE),
    Array$create(c(1L:9L, 0L), type = uint64())
  )
  expect_equal(
    as.vector(sort(ChunkedArray$create(tbl$int[1:5], tbl$int[6:10]))),
    sort(tbl$int, na.last = TRUE)
  )
  expect_equal(
    as.vector(sort(ChunkedArray$create(tbl$int[1:5], tbl$int[6:10]), decreasing = TRUE)),
    sort(tbl$int, decreasing = TRUE, na.last = TRUE)
  )
  expect_error(
    sort(ChunkedArray$create(tbl$int), decreasing = TRUE, na.last = NA),
    "na.last",
    fixed = TRUE
  )
})

test_that("Table/RecordBatch sort", {
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
