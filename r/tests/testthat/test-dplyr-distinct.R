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

tbl <- example_data
tbl$some_grouping <- rep(c(1, 2), 5)

test_that("distinct()", {
  expect_dplyr_equal(
    input %>%
      distinct(some_grouping, lgl) %>%
      collect(),
    tbl
  )
})

test_that("distinct() works without any variables", {
  expect_dplyr_equal(
    input %>%
      distinct() %>%
      collect(),
    tbl
  )
})

test_that("distinct() can retain groups", {
  skip("ARROW-13777 - internally uses mutate on grouped data; should work after this is merged")
  expect_dplyr_equal(
    input %>%
      group_by(some_grouping, false) %>%
      distinct(lgl) %>%
      collect() %>%
      arrange(lgl, some_grouping),
    tbl
  )
})

test_that("distinct() can contain expressions", {

  expect_dplyr_equal(
    input %>%
      distinct(lgl, x = some_grouping + 1) %>%
      collect(),
    tbl
  )

  skip("ARROW-13777 - internally uses mutate on grouped data; should work after this is merged")
  expect_dplyr_equal(
    input %>%
      group_by(lgl) %>%
      distinct(x = some_grouping + 1) %>%
      collect(),
    tbl
  )
})
