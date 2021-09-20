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

withr::local_options(list(arrow.summarise.sort = TRUE))

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
      summarize(min_int = min(int)) %>%
      collect(),
    tbl,
    warning = TRUE
  )

  expect_dplyr_equal(
    input %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      summarize(min_int = min(int) / 2) %>%
      collect(),
    tbl,
    warning = TRUE
  )
})

test_that("summarize() doesn't evaluate eagerly", {
  expect_s3_class(
    Table$create(tbl) %>%
      summarize(total = sum(int)),
    "arrow_dplyr_query"
  )
  expect_r6_class(
    Table$create(tbl) %>%
      summarize(total = sum(int)) %>%
      compute(),
    "ArrowTabular"
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
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(total = sum(int * 4, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(total = sum(int)) %>%
      collect(),
    tbl,
  )
})

test_that("Group by mean on dataset", {
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(mean = mean(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(mean = mean(int, na.rm = FALSE)) %>%
      collect(),
    tbl
  )
})

test_that("Group by sd on dataset", {
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(sd = sd(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(sd = sd(int, na.rm = FALSE)) %>%
      collect(),
    tbl
  )
})

test_that("Group by var on dataset", {
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(var = var(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(var = var(int, na.rm = FALSE)) %>%
      collect(),
    tbl
  )
})

test_that("n()", {
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
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(all(lgl, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(any(lgl, na.rm = FALSE)) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(all(lgl, na.rm = FALSE)) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      mutate(has_words = nchar(verses) < 0) %>%
      group_by(some_grouping) %>%
      summarize(any(has_words, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      mutate(has_words = nchar(verses) < 0) %>%
      group_by(some_grouping) %>%
      summarize(all(has_words, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(has_words = all(nchar(verses) < 0, na.rm = TRUE)) %>%
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
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      filter(int > 5) %>%
      group_by(some_grouping) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
})

test_that("Group by edge cases", {
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping * 2) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      group_by(alt = some_grouping * 2) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
})

test_that("Do things after summarize", {
  group2_sum <- tbl %>%
    group_by(some_grouping) %>%
    filter(int > 5) %>%
    summarize(total = sum(int, na.rm = TRUE)) %>%
    pull() %>%
    tail(1)

  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      filter(int > 5) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      filter(total == group2_sum) %>%
      mutate(extra = total * 5) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      filter(dbl > 2) %>%
      select(chr, int, lgl) %>%
      mutate(twice = int * 2L) %>%
      group_by(lgl) %>%
      summarize(
        count = n(),
        total = sum(twice, na.rm = TRUE)
      ) %>%
      mutate(mean = total / count) %>%
      collect(),
    tbl
  )
})

test_that("Expressions on aggregations", {
  # This is what it effectively is
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(
        any = any(lgl),
        all = all(lgl)
      ) %>%
      ungroup() %>% # TODO: loosen the restriction on mutate after group_by
      mutate(some = any & !all) %>%
      select(some_grouping, some) %>%
      collect(),
    tbl
  )
  # More concisely:
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(any(lgl) & !all(lgl)) %>%
      collect(),
    tbl
  )

  # Save one of the aggregates first
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(
        any_lgl = any(lgl),
        some = any_lgl & !all(lgl)
      ) %>%
      collect(),
    tbl
  )

  # Make sure order of columns in result is correct
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(
        any_lgl = any(lgl),
        some = any_lgl & !all(lgl),
        n()
      ) %>%
      collect(),
    tbl
  )

  # Aggregate on an aggregate (trivial but dplyr allows)
  skip("Aggregate on an aggregate not supported")
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(
        any_lgl = any(any(lgl))
      ) %>%
      collect(),
    tbl
  )
})

test_that("Summarize with 0 arguments", {
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize() %>%
      collect(),
    tbl
  )
})

test_that("Not (yet) supported: implicit join", {
  withr::local_options(list(arrow.debug = TRUE))
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(
        sum((dbl - mean(dbl))^2)
      ) %>%
      collect(),
    tbl,
    warning = "Expression sum\\(\\(dbl - mean\\(dbl\\)\\)\\^2\\) not supported in Arrow; pulling data into R"
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(
        sum(dbl - mean(dbl))
      ) %>%
      collect(),
    tbl,
    warning = "Expression sum\\(dbl - mean\\(dbl\\)\\) not supported in Arrow; pulling data into R"
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(
        sqrt(sum((dbl - mean(dbl))^2) / (n() - 1L))
      ) %>%
      collect(),
    tbl,
    warning = "Expression sum\\(\\(dbl - mean\\(dbl\\)\\)\\^2\\) not supported in Arrow; pulling data into R"
  )

  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(
        dbl - mean(dbl)
      ) %>%
      collect(),
    tbl,
    warning = "Expression dbl - mean\\(dbl\\) not supported in Arrow; pulling data into R"
  )

  # This one could possibly be supported--in mutate()
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      summarize(
        dbl - int
      ) %>%
      collect(),
    tbl,
    warning = "Expression dbl - int not supported in Arrow; pulling data into R"
  )
})

test_that(".groups argument", {
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping, int < 6) %>%
      summarize(count = n()) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping, int < 6) %>%
      summarize(count = n(), .groups = "drop_last") %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping, int < 6) %>%
      summarize(count = n(), .groups = "keep") %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping, int < 6) %>%
      summarize(count = n(), .groups = "drop") %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping, int < 6) %>%
      summarize(count = n(), .groups = "rowwise") %>%
      collect(),
    tbl,
    warning = TRUE
  )

  # abandon_ship() raises the warning, then dplyr itself errors
  # This isn't ideal but it's fine and won't be an issue on Datasets
  expect_error(
    expect_warning(
      Table$create(tbl) %>%
        group_by(some_grouping, int < 6) %>%
        summarize(count = n(), .groups = "NOTVALID"),
      "Invalid .groups argument"
    ),
    "NOTVALID"
  )
})
