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

skip_if_not_available("dataset")

library(dplyr)
library(stringr)

tbl <- example_data
# Add some better string data
tbl$verses <- verses[[1]]
# c(" a ", "  b  ", "   c   ", ...) increasing padding
# nchar =   3  5  7  9 11 13 15 17 19 21
tbl$padded_strings <- stringr::str_pad(letters[1:10], width = 2 * (1:10) + 1, side = "both")
tbl$some_grouping <- rep(c(1, 2), 5)

test_that("summarize", {
  expect_dplyr_equal(
    input %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      summarize(min_int = min(int)),
    tbl,
    warning = TRUE
  )

  expect_dplyr_equal(
    input %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      summarize(min_int = min(int) / 2),
    tbl,
    warning = TRUE
  )
})

test_that("Can aggregate in Arrow", {
  expect_dplyr_equal(
    input %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      summarize(total = sum(int)) %>%
      collect(),
    tbl
  )
})

test_that("Group by sum on dataset", {
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(total = sum(int * 4, na.rm = TRUE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(total = sum(int)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl,
  )
})

test_that("Group by mean on dataset", {
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(mean = mean(int, na.rm = TRUE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(mean = mean(int, na.rm = FALSE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )
})

test_that("Group by sd on dataset", {
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(sd = sd(int, na.rm = TRUE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )

  skip("ARROW-13691 - na.rm not yet implemented for VarianceOptions")
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(sd = sd(int, na.rm = FALSE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )
})

test_that("Group by var on dataset", {
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(var = var(int, na.rm = TRUE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )

  skip("ARROW-13691 - na.rm not yet implemented for VarianceOptions")
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(var = var(int, na.rm = FALSE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )
})

test_that("n()", {
  withr::local_options(list(arrow.debug = TRUE))
  expect_dplyr_equal(
    input %>%
      summarize(counts = n()) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(counts = n()) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )
})

test_that("Group by any/all", {
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(any(lgl, na.rm = TRUE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(all(lgl, na.rm = TRUE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(any(lgl, na.rm = FALSE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(all(lgl, na.rm = FALSE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      mutate(has_words = nchar(verses) < 0) %>%
      group_by(some_grouping) %>%
      summarize(any(has_words, na.rm = TRUE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      mutate(has_words = nchar(verses) < 0) %>%
      group_by(some_grouping) %>%
      summarize(all(has_words, na.rm = TRUE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(has_words = all(nchar(verses) < 0, na.rm = TRUE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )
})

test_that("Group by n_distinct() on dataset", {
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(distinct = n_distinct(lgl, na.rm = FALSE)) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(distinct = n_distinct(lgl, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
})

test_that("Filter and aggregate", {
  expect_dplyr_equal(
    input %>%
      filter(some_grouping == 2) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      filter(int > 5) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      filter(some_grouping == 2) %>%
      group_by(some_grouping) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      filter(int > 5) %>%
      group_by(some_grouping) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )
})
