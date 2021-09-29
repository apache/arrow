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

library(dplyr, warn.conflicts = FALSE)
library(stringr)

tbl <- example_data

test_that("group_by groupings are recorded", {
  expect_dplyr_equal(
    input %>%
      group_by(chr) %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      collect(),
    tbl
  )
})

test_that("group_by supports creating/renaming", {
  expect_dplyr_equal(
    input %>%
      group_by(chr, numbers = int) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(chr, numbers = int * 4) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      group_by(int > 4, lgl, foo = int > 5) %>%
      collect(),
    tbl
  )
})

test_that("ungroup", {
  expect_dplyr_equal(
    input %>%
      group_by(chr) %>%
      select(int, chr) %>%
      ungroup() %>%
      filter(int > 5) %>%
      collect(),
    tbl
  )

  # to confirm that the above expectation is actually testing what we think it's
  # testing, verify that expect_dplyr_equal() distinguishes between grouped and
  # ungrouped tibbles
  expect_error(
    expect_dplyr_equal(
      input %>%
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
  expect_dplyr_equal(
    input %>%
      group_by(chr) %>%
      select(string = chr, int) %>%
      collect(),
    tbl
  )
})

test_that("group_by with .drop", {
  test_groups <- c("starting_a_fight", "consoling_a_child", "petting_a_dog")
  expect_dplyr_equal(
    input %>%
      group_by(!!!syms(test_groups), .drop = TRUE) %>%
      collect(),
    example_with_logical_factors
  )
  expect_dplyr_equal(
    input %>%
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
  expect_dplyr_equal(
    input %>%
      group_by(.drop = FALSE) %>% # no group by vars
      group_by_drop_default(),
    example_with_logical_factors
  )
  expect_dplyr_equal(
    input %>%
      group_by_drop_default(),
    example_with_logical_factors
  )
  expect_dplyr_equal(
    input %>%
      group_by(!!!syms(test_groups)) %>%
      group_by_drop_default(),
    example_with_logical_factors
  )
  expect_dplyr_equal(
    input %>%
      group_by(!!!syms(test_groups), .drop = FALSE) %>%
      ungroup() %>%
      group_by_drop_default(),
    example_with_logical_factors
  )
})
