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

expect_dplyr_equal <- function(expr, # A dplyr pipeline with `input` as its start
                               tbl,  # A tbl/df as reference, will make RB/Table with
                               skip_record_batch = NULL, # Msg, if should skip RB test
                               skip_table = NULL,        # Msg, if should skip Table test
                               ...) {
  expr <- rlang::enquo(expr)
  expected <- rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(input = tbl)))

  if (is.null(skip_record_batch)) {
    via_batch <- rlang::eval_tidy(
      expr,
      rlang::new_data_mask(rlang::env(input = record_batch(tbl)))
    )
    expect_equivalent(via_batch, expected, ...)
  } else {
    skip(skip_record_batch)
  }

  if (is.null(skip_table)) {
    via_table <- rlang::eval_tidy(
      expr,
      rlang::new_data_mask(rlang::env(input = Table$create(tbl)))
    )
    expect_equivalent(via_table, expected, ...)
  } else {
    skip(skip_table)
  }
}

expect_dplyr_error <- function(expr, # A dplyr pipeline with `input` as its start
                               tbl,  # A tbl/df as reference, will make RB/Table with
                               ...) {
  expr <- rlang::enquo(expr)
  msg <- tryCatch(
    rlang::eval_tidy(expr, rlang::new_data_mask(rlang::env(input = tbl))),
    error = function (e) conditionMessage(e)
  )
  expect_is(msg, "character", label = "dplyr on data.frame did not error")

  expect_error(
    rlang::eval_tidy(
      expr,
      rlang::new_data_mask(rlang::env(input = record_batch(tbl)))
    ),
    msg,
    ...
  )
  expect_error(
    rlang::eval_tidy(
      expr,
      rlang::new_data_mask(rlang::env(input = Table$create(tbl)))
    ),
    msg,
    ...
  )
}

tbl <- example_data

test_that("basic select/filter/collect", {
  batch <- record_batch(tbl)

  b2 <- batch %>%
    select(int, chr) %>%
    filter(int > 5)

  expect_is(b2, "arrow_dplyr_query")
  t2 <- collect(b2)
  expect_equal(t2, tbl[tbl$int > 5 & !is.na(tbl$int), c("int", "chr")])
  # Test that the original object is not affected
  expect_identical(collect(batch), tbl)
})

test_that("filter() on is.na()", {
  expect_dplyr_equal(
    input %>%
      filter(is.na(lgl)) %>%
      select(chr, int, lgl) %>%
      collect(),
    tbl
  )
})

test_that("filter() with NAs in selection", {
  expect_dplyr_equal(
    input %>%
      filter(lgl) %>%
      select(chr, int, lgl) %>%
      collect(),
    tbl
  )
})

test_that("Filter returning an empty Table should not segfault (ARROW-8354)", {
  expect_dplyr_equal(
    input %>%
      filter(false) %>%
      select(chr, int, lgl) %>%
      collect(),
    tbl
  )
})

test_that("filtering with expression", {
  char_sym <- "b"
  expect_dplyr_equal(
    input %>%
      filter(chr == char_sym) %>%
      select(string = chr, int) %>%
      collect(),
    tbl
  )
})

test_that("More complex select/filter", {
  expect_dplyr_equal(
    input %>%
      filter(dbl > 2, chr == "d" | chr == "f") %>%
      select(chr, int, lgl) %>%
      filter(int < 5) %>%
      select(int, chr) %>%
      collect(),
    tbl
  )
})

test_that("filter() with %in%", {
  expect_dplyr_equal(
    input %>%
      filter(dbl > 2, chr %in% c("d", "f")) %>%
      collect(),
    tbl
  )
})

test_that("filter environment scope", {
  # "object 'b_var' not found"
  expect_dplyr_error(input %>% filter(batch, chr == b_var))

  b_var <- "b"
  expect_dplyr_equal(
    input %>%
      filter(chr == b_var) %>%
      collect(),
    tbl
  )
  # Also for functions
  # 'could not find function "isEqualTo"'
  expect_dplyr_error(filter(batch, isEqualTo(int, 4)))

  isEqualTo <- function(x, y) x == y & !is.na(x)
  expect_dplyr_equal(
    input %>%
      select(-fct) %>% # factor levels aren't identical
      filter(isEqualTo(int, 4)) %>%
      collect(),
    tbl
  )
})

test_that("Filtering on a column that doesn't exist errors correctly", {
  skip("Error handling in filter() needs to be internationalized")
  expect_error(
    batch %>% filter(not_a_col == 42) %>% collect(),
    "object 'not_a_col' not found"
  )
})

test_that("Filtering with a function that doesn't have an Array/expr method still works", {
  expect_warning(
    expect_dplyr_equal(
      input %>%
        filter(int > 2, pnorm(dbl) > .99) %>%
        collect(),
      tbl
    ),
    'Filter expression not implemented in Arrow: pnorm(dbl) > 0.99; pulling data into R',
    fixed = TRUE
  )
})

test_that("summarize", {
  expect_dplyr_equal(
    input %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      summarize(min_int = min(int)),
    tbl
  )
})

test_that("mutate", {
  expect_dplyr_equal(
    input %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      mutate(int = int + 6L) %>%
      summarize(min_int = min(int)),
    tbl
  )
})

test_that("transmute", {
  skip("TODO: reimplement transmute (with dplyr 1.0, it no longer just works via mutate)")
  expect_dplyr_equal(
    input %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      transmute(int = int + 6L) %>%
      summarize(min_int = min(int)),
    tbl
  )
})

test_that("group_by groupings are recorded", {
  expect_dplyr_equal(
    input %>%
      group_by(chr) %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      summarize(min_int = min(int)),
    tbl
  )
  # Test that the original object is not affected
  expect_identical(collect(batch), tbl)
})

test_that("ungroup", {
  expect_dplyr_equal(
    input %>%
      group_by(chr) %>%
      select(int, chr) %>%
      ungroup() %>%
      filter(int > 5) %>%
      summarize(min_int = min(int)),
    tbl
  )
  # Test that the original object is not affected
  expect_identical(collect(batch), tbl)
})

test_that("Empty select returns no columns", {
  expect_dplyr_equal(
    input %>% select() %>% collect(),
    tbl,
    skip_table = "Table with 0 cols doesn't know how many rows it should have"
  )
})
test_that("Empty select still includes the group_by columns", {
  expect_dplyr_equal(
    input %>% group_by(chr) %>% select() %>% collect(),
    tbl
  )
})

test_that("arrange", {
  expect_dplyr_equal(
    input %>%
      group_by(chr) %>%
      select(int, chr) %>%
      arrange(desc(int)) %>%
      collect(),
    tbl
  )
})

test_that("select/rename", {
  expect_dplyr_equal(
    input %>%
      select(string = chr, int) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      rename(string = chr) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      rename(strng = chr) %>%
      rename(other = strng) %>%
      collect(),
    tbl
  )
})

test_that("filtering with rename", {
  expect_dplyr_equal(
    input %>%
      filter(chr == "b") %>%
      select(string = chr, int) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      select(string = chr, int) %>%
      filter(string == "b") %>%
      collect(),
    tbl
  )
})

test_that("group_by then rename", {
  expect_dplyr_equal(
    input %>%
      group_by(chr) %>%
      select(string = chr, int) %>%
      collect(),
    tbl
  )
})

test_that("pull", {
  expect_dplyr_equal(
    input %>% pull(),
    tbl
  )
  expect_dplyr_equal(
    input %>% pull(1),
    tbl
  )
  expect_dplyr_equal(
    input %>% pull(chr),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      filter(int > 4) %>%
      rename(strng = chr) %>%
      pull(strng),
    tbl
  )
})
