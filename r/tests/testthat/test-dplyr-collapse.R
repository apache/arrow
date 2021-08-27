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
library(stringr)

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
    schema(numbers = float64(), words = utf8())
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