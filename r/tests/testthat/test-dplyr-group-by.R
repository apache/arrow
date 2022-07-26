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

skip_if(on_old_windows())

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
