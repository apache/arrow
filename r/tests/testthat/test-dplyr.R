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

context("dplyr verbs")

library(dplyr)

test_that("basic select/filter/collect", {
  # Note that these get recycled throughout the tests
  tbl <- tibble::tibble(
    int = 1:10,
    dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10],
    fct = factor(letters[1:10])
  )
  batch <- record_batch(tbl)

  b2 <- batch %>%
    select(int, chr) %>%
    filter(int > 5)

  expect_is(b2, "RecordBatch")
  t2 <- collect(b2)
  expect_equal(t2, tbl[tbl$int > 5, c("int", "chr")])
  # Test that the original object is not affected
  expect_identical(collect(batch), tbl)
})

test_that("More complex select/filter", {
  out <- batch %>%
    filter(dbl > 2, chr %in% c("d", "f")) %>%
    select(chr, int, lgl) %>%
    filter(int < 5) %>%
    select(int, chr) %>%
    collect()
  expect_equal(out, tbl[4, c("int", "chr")])
})

test_that("summarize on RecordBatch works", {
  m_i <- batch %>%
    select(int, chr) %>%
    filter(int > 5) %>%
    summarize(min_int = min(int))
  expect_identical(m_i$min_int, 6L)
})

test_that("mutate", {
  m_i <- batch %>%
    select(int, chr) %>%
    filter(int > 5) %>%
    mutate(int = int + 6L) %>%
    summarize(min_int = min(int))
  expect_identical(m_i$min_int, 12L)
})

test_that("group_by groupings are recorded", {
  m_i <- batch %>%
    group_by(chr) %>%
    select(int, chr) %>%
    filter(int > 5) %>%
    summarize(min_int = min(int))
  expect_identical(m_i,
    tibble::tibble(
      chr = tbl$chr[tbl$int > 5],
      min_int = tbl$int[tbl$int > 5]
    )
  )
  # Test that the original object is not affected
  expect_identical(collect(batch), tbl)
})
