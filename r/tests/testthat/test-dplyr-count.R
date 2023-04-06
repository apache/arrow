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

skip_if_not_available("acero")

tbl <- example_data
tbl$some_grouping <- rep(c(1, 2), 5)
tbl$another_grouping <- rep(c(1, 2), 5)

test_that("count/tally", {
  compare_dplyr_binding(
    .input %>%
      count() %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      tally() %>%
      collect(),
    tbl
  )
})

test_that("count/tally with wt and grouped data", {
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      count(wt = int) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      tally(wt = int) %>%
      collect(),
    tbl
  )
})

test_that("count/tally with sort", {
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      count(wt = int, sort = TRUE) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      tally(wt = int, sort = TRUE) %>%
      collect(),
    tbl
  )
})

test_that("count/tally with name arg", {
  compare_dplyr_binding(
    .input %>%
      count(name = "new_col") %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      tally(name = "new_col") %>%
      collect(),
    tbl
  )
})

test_that("count returns an ungrouped tibble", {
  compare_dplyr_binding(
    .input %>%
      count(some_grouping, another_grouping, sort = TRUE) %>%
      collect(),
    tbl
  )
})

test_that("tally raises appropriate error and message for names", {

  expect_message(
    tbl %>%
      arrow_table() %>%
      rename(n = some_grouping) %>%
      group_by(n) %>%
      tally() %>%
      collect(),
    regexp = 'Use `name = "new_name"` to pick a new name.',
    fixed = TRUE
  )

  expect_error(
    tbl %>%
      arrow_table() %>%
      group_by(some_grouping) %>%
      tally(wt = int, name = 99) %>%
      collect(),
    "`name` must be a string or `NULL`.",
    fixed = TRUE
  )
})
