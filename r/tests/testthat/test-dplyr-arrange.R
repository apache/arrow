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

# randomize order of rows in test data
tbl <- slice_sample(example_data_for_sorting, prop = 1L)

test_that("arrange() on integer, double, and character columns", {
  expect_dplyr_equal(
    input %>%
      arrange(int, chr) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      arrange(int, desc(dbl)) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      arrange(int, desc(desc(dbl))) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      arrange(int) %>%
      arrange(desc(dbl)) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      arrange(int + dbl, chr) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      mutate(zzz = int + dbl,) %>%
      arrange(zzz, chr) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      mutate(zzz = int + dbl) %>%
      arrange(int + dbl, chr) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      mutate(int + dbl) %>%
      arrange(int + dbl, chr) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(grp) %>%
      arrange(int, dbl) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(grp) %>%
      arrange(int, dbl, .by_group = TRUE) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(grp, grp2) %>%
      arrange(int, dbl, .by_group = TRUE) %>%
      collect(),
    tbl %>%
      mutate(grp2 = ifelse(is.na(lgl), 1L, as.integer(lgl)))
  )
  expect_dplyr_equal(
    input %>%
      group_by(grp) %>%
      arrange(.by_group = TRUE) %>%
      pull(grp),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      arrange() %>%
      collect(),
    tbl %>%
      group_by(grp)
  )
  expect_dplyr_equal(
    input %>%
      group_by(grp) %>%
      arrange() %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      arrange() %>%
      collect(),
    tbl
  )
  test_sort_col <- "chr"
  expect_dplyr_equal(
    input %>%
      arrange(!!sym(test_sort_col)) %>%
      collect(),
    tbl %>%
      select(chr, lgl)
  )
  test_sort_cols <- c("int", "dbl")
  expect_dplyr_equal(
    input %>%
      arrange(!!!syms(test_sort_cols)) %>%
      collect(),
    tbl
  )
})

test_that("arrange() on datetime columns", {
  expect_dplyr_equal(
    input %>%
      arrange(dttm, int) %>%
      collect(),
    tbl
  )
  skip("Sorting by only a single timestamp column fails (ARROW-12087)")
  expect_dplyr_equal(
    input %>%
      arrange(dttm) %>%
      collect(),
    tbl %>%
      select(dttm, grp)
  )
})

test_that("arrange() on logical columns", {
  expect_dplyr_equal(
    input %>%
      arrange(lgl, int) %>%
      collect(),
    tbl
  )
})

test_that("arrange() with bad inputs", {
  expect_error(
    tbl %>%
      Table$create() %>%
      arrange(1),
    "does not contain any field names",
    fixed = TRUE
  )
  expect_error(
    tbl %>%
      Table$create() %>%
      arrange(2 + 2),
    "does not contain any field names",
    fixed = TRUE
  )
  expect_error(
    tbl %>%
      Table$create() %>%
      arrange(aertidjfgjksertyj),
    "not found",
    fixed = TRUE
  )
  expect_error(
    tbl %>%
      Table$create() %>%
      arrange(desc(aertidjfgjksertyj + iaermxiwerksxsdqq)),
    "not found",
    fixed = TRUE
  )
  expect_error(
    tbl %>%
      Table$create() %>%
      arrange(desc(int, chr)),
    "expects only one argument",
    fixed = TRUE
  )
})
