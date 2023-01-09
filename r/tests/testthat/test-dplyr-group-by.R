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
library(stringr)

tbl <- example_data

test_that("group_by groupings are recorded", {
  compare_dplyr_binding(
    .input %>%
      group_by(chr) %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      collect(),
    tbl
  )
})

test_that("group_by supports creating/renaming", {
  compare_dplyr_binding(
    .input %>%
      group_by(chr, numbers = int) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(chr, numbers = int * 4) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(int > 4, lgl, foo = int > 5) %>%
      collect(),
    tbl
  )
})

test_that("group_by supports re-grouping by overlapping groups", {
  compare_dplyr_binding(
    .input %>%
      group_by(chr, int) %>%
      group_by(int, dbl) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      group_by(chr, int) %>%
      group_by(int, chr = "some new value") %>%
      collect(),
    tbl
  )
})

test_that("ungroup", {
  compare_dplyr_binding(
    .input %>%
      group_by(chr) %>%
      select(int, chr) %>%
      ungroup() %>%
      filter(int > 5) %>%
      collect(),
    tbl
  )

  # to confirm that the above expectation is actually testing what we think it's
  # testing, verify that compare_dplyr_binding() distinguishes between grouped and
  # ungrouped tibbles
  expect_error(
    compare_dplyr_binding(
      .input %>%
        group_by(chr) %>%
        select(int, chr) %>%
        (function(x) if (inherits(x, "tbl_df")) ungroup(x) else x) %>%
        filter(int > 5) %>%
        collect(),
      tbl
    )
  )
})

test_that("Groups before conversion to a Table must not be restored after collect() (ARROW-17737)", {
  compare_dplyr_binding(
    .input %>%
      group_by(chr, .add = FALSE) %>%
      ungroup() %>%
      collect(),
    tbl %>%
      group_by(int)
  )
  compare_dplyr_binding(
    .input %>%
      group_by(chr, .add = TRUE) %>%
      ungroup() %>%
      collect(),
    tbl %>%
      group_by(int)
  )
})

test_that("group_by then rename", {
  compare_dplyr_binding(
    .input %>%
      group_by(chr) %>%
      select(string = chr, int) %>%
      collect(),
    tbl
  )
})

test_that("group_by with .drop", {
  test_groups <- c("starting_a_fight", "consoling_a_child", "petting_a_dog")
  compare_dplyr_binding(
    .input %>%
      group_by(!!!syms(test_groups), .drop = TRUE) %>%
      collect(),
    example_with_logical_factors
  )
  compare_dplyr_binding(
    .input %>%
      group_by(!!!syms(test_groups), .drop = FALSE) %>%
      collect(),
    example_with_logical_factors
  )
  expect_equal(
    example_with_logical_factors %>%
      group_by(!!!syms(test_groups), .drop = TRUE) %>%
      collect() %>%
      n_groups(),
    4L
  )
  expect_equal(
    example_with_logical_factors %>%
      group_by(!!!syms(test_groups), .drop = FALSE) %>%
      collect() %>%
      n_groups(),
    8L
  )
  expect_equal(
    example_with_logical_factors %>%
      group_by(!!!syms(test_groups), .drop = FALSE) %>%
      group_by_drop_default(),
    FALSE
  )
  expect_equal(
    example_with_logical_factors %>%
      group_by(!!!syms(test_groups), .drop = TRUE) %>%
      group_by_drop_default(),
    TRUE
  )
  compare_dplyr_binding(
    .input %>%
      group_by(.drop = FALSE) %>% # no group by vars
      group_by_drop_default(),
    example_with_logical_factors
  )
  compare_dplyr_binding(
    .input %>%
      group_by_drop_default(),
    example_with_logical_factors
  )
  compare_dplyr_binding(
    .input %>%
      group_by(!!!syms(test_groups)) %>%
      group_by_drop_default(),
    example_with_logical_factors
  )
  compare_dplyr_binding(
    .input %>%
      group_by(!!!syms(test_groups), .drop = FALSE) %>%
      ungroup() %>%
      group_by_drop_default(),
    example_with_logical_factors
  )
})

test_that("group_by() with namespaced functions", {
  compare_dplyr_binding(
    .input %>%
      group_by(int > base::sqrt(25)) %>%
      summarise(mean(dbl, na.rm = TRUE)) %>%
      # group order is different from dplyr, hence reordering
      arrange(`int > base::sqrt(25)`) %>%
      collect(),
    tbl
  )
})

test_that("group_by() with .add", {
  compare_dplyr_binding(
    .input %>%
      group_by(dbl2) %>%
      group_by(.add = FALSE) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(dbl2) %>%
      group_by(.add = TRUE) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(dbl2) %>%
      group_by(chr, .add = FALSE) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(dbl2) %>%
      group_by(chr, .add = TRUE) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(.add = FALSE) %>%
      collect(),
    tbl %>%
      group_by(dbl2)
  )
  compare_dplyr_binding(
    .input %>%
      group_by(.add = TRUE) %>%
      collect(),
    tbl %>%
      group_by(dbl2)
  )
  compare_dplyr_binding(
    .input %>%
      group_by(chr, .add = FALSE) %>%
      collect(),
    tbl %>%
      group_by(dbl2)
  )
  compare_dplyr_binding(
    .input %>%
      group_by(chr, .add = TRUE) %>%
      collect(),
    tbl %>%
      group_by(dbl2)
  )
  suppressWarnings(compare_dplyr_binding(
    .input %>%
      group_by(dbl2) %>%
      group_by(add = FALSE) %>%
      collect(),
    tbl,
    warning = "deprecated"
  ))
  suppressWarnings(compare_dplyr_binding(
    .input %>%
      group_by(dbl2) %>%
      group_by(add = TRUE) %>%
      collect(),
    tbl,
    warning = "deprecated"
  ))
  expect_warning(
    tbl %>%
      arrow_table() %>%
      group_by(add = TRUE) %>%
      collect(),
    "The `add` argument of `group_by\\(\\)` is deprecated"
  )
  expect_error(
    suppressWarnings(
      tbl %>%
        arrow_table() %>%
        group_by(add = dbl2) %>%
        collect()
    ),
    "object 'dbl2' not found"
  )
})

test_that("Can use across() within group_by()", {
  test_groups <- c("dbl", "int", "chr")
  compare_dplyr_binding(
    .input %>%
      group_by(across(everything())) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(across(starts_with("d"))) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(across({{ test_groups }})) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      group_by(across(where(is.numeric))) %>%
      collect(),
    tbl
  )
})

test_that("ARROW-18131 - correctly handles .data pronoun in group_by()", {
  compare_dplyr_binding(
    .input %>%
      group_by(.data$lgl) %>%
      collect(),
    tbl
  )
})
