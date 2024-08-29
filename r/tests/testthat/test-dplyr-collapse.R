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

withr::local_options(list(arrow.summarise.sort = TRUE))

library(dplyr, warn.conflicts = FALSE)
library(stringr)

skip_if_not_available("acero")

tbl <- example_data
# Add some better string data
tbl$verses <- verses[[1]]
# c(" a ", "  b  ", "   c   ", ...) increasing padding
# nchar =   3  5  7  9 11 13 15 17 19 21
tbl$padded_strings <- stringr::str_pad(letters[1:10], width = 2 * (1:10) + 1, side = "both")
tbl$some_grouping <- rep(c(1, 2), 5)

tab <- Table$create(tbl)


test_that("implicit_schema with select", {
  expect_equal(
    tab %>%
      select(int, lgl) %>%
      implicit_schema(),
    schema(int = int32(), lgl = bool())
  )
})

test_that("implicit_schema with rename", {
  expect_equal(
    tab %>%
      select(numbers = int, lgl) %>%
      implicit_schema(),
    schema(numbers = int32(), lgl = bool())
  )
})

test_that("implicit_schema with mutate", {
  expect_equal(
    tab %>%
      transmute(
        numbers = int * 4,
        words = as.character(int)
      ) %>%
      implicit_schema(),
    schema(numbers = int32(), words = utf8())
  )
})

test_that("implicit_schema with summarize", {
  expect_equal(
    tab %>%
      summarize(
        avg = mean(int)
      ) %>%
      implicit_schema(),
    schema(avg = float64())
  )
})

test_that("implicit_schema with group_by summarize", {
  expect_equal(
    tab %>%
      group_by(some_grouping) %>%
      summarize(
        avg = mean(int * 5L)
      ) %>%
      implicit_schema(),
    schema(some_grouping = float64(), avg = float64())
  )
})

test_that("collapse", {
  q <- tab %>%
    filter(dbl > 2, chr == "d" | chr == "f") %>%
    select(chr, int, lgl) %>%
    mutate(twice = int * 2L)
  expect_false(is_collapsed(q))
  expect_true(is_collapsed(collapse(q)))
  expect_false(is_collapsed(collapse(q)$.data))

  compare_dplyr_binding(
    .input %>%
      filter(dbl > 2, chr == "d" | chr == "f") %>%
      select(chr, int, lgl) %>%
      mutate(twice = int * 2L) %>%
      collapse() %>%
      filter(int < 5) %>%
      select(int, twice) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(dbl > 2, chr == "d" | chr == "f") %>%
      collapse() %>%
      select(chr, int, lgl) %>%
      collapse() %>%
      filter(int < 5) %>%
      select(int, chr) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(dbl > 2, chr == "d" | chr == "f") %>%
      collapse() %>%
      group_by(chr) %>%
      select(chr, int, lgl) %>%
      collapse() %>%
      filter(int < 5) %>%
      select(int, chr) %>%
      collect(),
    tbl
  )
})

test_that("Properties of collapsed query", {
  q <- tab %>%
    filter(dbl > 2) %>%
    select(chr, int, lgl) %>%
    mutate(twice = int * 2L) %>%
    group_by(lgl) %>%
    summarize(total = sum(int, na.rm = TRUE)) %>%
    mutate(extra = total * 5)

  # print(tbl %>%
  #   filter(dbl > 2) %>%
  #   select(chr, int, lgl) %>%
  #   mutate(twice = int * 2L) %>%
  #   group_by(lgl) %>%
  #   summarize(total = sum(int, na.rm = TRUE)) %>%
  #   mutate(extra = total * 5))

  #   # A tibble: 3 × 3
  #   lgl   total extra
  #   <lgl> <int> <dbl>
  # 1 FALSE     8    40
  # 2 TRUE      8    40
  # 3 NA       25   125

  # Avoid evaluating just for nrow
  expect_identical(dim(q), c(NA_integer_, 3L))

  expect_output(
    print(q),
    "Table (query)
lgl: bool
total: int64
extra: int64 (multiply_checked(total, 5))

* Sorted by lgl [asc]
See $.data for the source Arrow object",
    fixed = TRUE
  )
  expect_output(
    print(q$.data),
    "Table (query)
int: int32
lgl: bool

* Aggregations:
total: sum(int)
* Filter: (dbl > 2)
* Grouped by lgl
See $.data for the source Arrow object",
    fixed = TRUE
  )

  skip_if(getRversion() < "3.6.0", "TODO investigate why these aren't equal")
  # On older R versions:
  #  ── Failure (test-dplyr-collapse.R:172:3): Properties of collapsed query ────────
  # head(q, 1) %>% collect() not equal to tibble::tibble(lgl = FALSE, total = 8L, extra = 40).
  # Component "total": Mean relative difference: 0.3846154
  # Component "extra": Mean relative difference: 0.3846154
  # ── Failure (test-dplyr-collapse.R:176:3): Properties of collapsed query ────────
  # tail(q, 1) %>% collect() not equal to tibble::tibble(lgl = NA, total = 25L, extra = 125).
  # Component "total": Mean relative difference: 0.9230769
  # Component "extra": Mean relative difference: 0.9230769
  expect_equal(
    q %>%
      arrange(lgl) %>%
      head(1) %>%
      collect(),
    tibble::tibble(lgl = FALSE, total = 8L, extra = 40)
  )
  skip("TODO (ARROW-16630): make sure BottomK can handle NA ordering")
  expect_equal(
    q %>%
      arrange(lgl) %>%
      tail(1) %>%
      collect(),
    tibble::tibble(lgl = NA, total = 25L, extra = 125)
  )
})

test_that("query_on_dataset handles collapse()", {
  expect_false(query_on_dataset(
    tab %>%
      select(int, chr)
  ))
  expect_false(query_on_dataset(
    tab %>%
      select(int, chr) %>%
      collapse() %>%
      select(int)
  ))

  skip_if_not_available("dataset")

  ds_dir <- tempfile()
  dir.create(ds_dir)
  on.exit(unlink(ds_dir))
  write_parquet(tab, file.path(ds_dir, "file.parquet"))
  ds <- open_dataset(ds_dir)

  expect_true(query_on_dataset(
    ds %>%
      select(int, chr)
  ))
  expect_true(query_on_dataset(
    ds %>%
      select(int, chr) %>%
      collapse() %>%
      select(int)
  ))
})

test_that("collapse doesn't unnecessarily add ProjectNodes", {
  plan <- capture.output(
    tab %>%
      collapse() %>%
      collapse() %>%
      show_query()
  )
  # There should be no projections
  expect_length(grep("ProjectNode", plan), 0)

  plan <- capture.output(
    tab %>%
      select(int, chr) %>%
      collapse() %>%
      collapse() %>%
      show_query()
  )
  # There should be just one projection
  expect_length(grep("ProjectNode", plan), 1)

  skip_if_not_available("dataset")
  # We need one ProjectNode on dataset queries to handle augmented fields

  tf <- tempfile()
  write_dataset(tab, tf, partitioning = "lgl")
  ds <- open_dataset(tf)

  plan <- capture.output(
    ds %>%
      collapse() %>%
      collapse() %>%
      show_query()
  )
  expect_length(grep("ProjectNode", plan), 1)
})
