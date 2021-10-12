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

tbl <- example_data
tbl$some_grouping <- rep(c(1, 2), 5)

test_that("count/tally", {
  expect_dplyr_equal(
    input %>%
      count() %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      tally() %>%
      collect(),
    tbl
  )
})

test_that("count/tally with wt and grouped data", {
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      count(wt = int) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      tally(wt = int) %>%
      collect(),
    tbl
  )
})

test_that("count/tally with sort", {
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      count(wt = int, sort = TRUE) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      group_by(some_grouping) %>%
      tally(wt = int, sort = TRUE) %>%
      collect(),
    tbl
  )
})

test_that("count/tally with name arg", {
  expect_dplyr_equal(
    input %>%
      count(name = "new_col") %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      tally(name = "new_col") %>%
      collect(),
    tbl
  )
})
