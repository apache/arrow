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

library(lubridate)
library(dplyr)

test_date <- as.POSIXct("2017-01-01 00:00:12.3456789", tz = "")
test_df <- tibble::tibble(date = test_date)

test_that("extract year from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = year(date)) %>%
      collect(),
    test_df,
    check.tzone = FALSE
  )
})

test_that("extract isoyear from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = isoyear(date)) %>%
      collect(),
    test_df
  )
})
  
test_that("extract quarter from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = quarter(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract month from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = month(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract isoweek from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = isoweek(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract day from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = day(date)) %>%
      collect(),
    test_df
  )
})
  

test_that("extract wday from date", {
 expect_dplyr_equal(
    input %>%
      mutate(x = wday(date)) %>%
      collect(),
    test_df
  )
  
  expect_dplyr_equal(
    input %>%
      mutate(x = wday(date, week_start = 3)) %>%
      collect(),
    test_df
  )
  
  expect_dplyr_equal(
    input %>%
      mutate(x = wday(date, week_start = 1)) %>%
      collect(),
    test_df
  )
  
  expect_warning(
    test_df %>%
      Table$create() %>%
      mutate(x = wday(date, label = TRUE)) %>%
      collect(),
    # Update this test after ARROW-13133 is resolved
    regexp = "Label argument not supported by Arrow; pulling data into R"
  )
  
  expect_warning(
    test_df %>%
      Table$create() %>%
      mutate(x = wday(date, locale = Sys.getlocale("LC_TIME"))) %>%
      collect(),
    regexp = 'Expression wday(date, locale = Sys.getlocale("LC_TIME")) not supported in Arrow; pulling data into R',
    fixed = TRUE
  )
})
  
test_that("extract yday from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = yday(date)) %>%
      collect(),
    test_df
  )
})
  
test_that("extract hour from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = hour(date)) %>%
      collect(),
    test_df
  )
})
  
test_that("extract minute from date", {
   expect_dplyr_equal(
    input %>%
      mutate(x = minute(date)) %>%
      collect(),
    test_df
  )
})
  
test_that("extract second from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = second(date)) %>%
      collect(),
    test_df,
    # arrow supports nanosecond resolution but lubridate does not
    tolerance = 1e-6
  )
})

