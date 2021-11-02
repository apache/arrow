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

library(lubridate, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

# base::strptime() defaults to local timezone
# but arrow's strptime defaults to UTC.
# So that tests are consistent, set the local timezone to UTC
# TODO: consider reevaluating this workaround after ARROW-12980
withr::local_timezone("UTC")

# TODO: We should test on windows once ARROW-13168 is resolved.
if (tolower(Sys.info()[["sysname"]]) == "windows") {
  test_date <- as.POSIXct("2017-01-01 00:00:11.3456789", tz = "")
} else {
  test_date <- as.POSIXct("2017-01-01 00:00:11.3456789", tz = "Pacific/Marquesas")
}


test_df <- tibble::tibble(
  # test_date + 1 turns the tzone = "" to NULL, which is functionally equivalent
  # so we can run some tests on Windows, but this skirts around
  # https://issues.apache.org/jira/browse/ARROW-13588
  # That issue is tough because in C++, "" is the "no timezone" value
  # due to static typing, so we can't distinguish a literal "" from NULL
  datetime = c(test_date, NA) + 1,
  date = c(as.Date("2021-09-09"), NA)
)

# These tests test component extraction from timestamp objects

test_that("extract year from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = year(datetime)) %>%
      collect(),
    test_df
  )
})

test_that("extract isoyear from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = isoyear(datetime)) %>%
      collect(),
    test_df
  )
})

test_that("extract quarter from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = quarter(datetime)) %>%
      collect(),
    test_df
  )
})

test_that("extract month from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = month(datetime)) %>%
      collect(),
    test_df
  )
})

test_that("extract isoweek from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = isoweek(datetime)) %>%
      collect(),
    test_df
  )
})

test_that("extract epiweek from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = epiweek(datetime)) %>%
      collect(),
    test_df
  )
})

test_that("extract day from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = day(datetime)) %>%
      collect(),
    test_df
  )
})

test_that("extract wday from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = wday(datetime)) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = wday(date, week_start = 3)) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = wday(date, week_start = 1)) %>%
      collect(),
    test_df
  )

  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-13168

  compare_dplyr_binding(
    .input %>%
      mutate(x = wday(date, label = TRUE)) %>%
      mutate(x = as.character(x)) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = wday(datetime, label = TRUE, abbr = TRUE)) %>%
      mutate(x = as.character(x)) %>%
      collect(),
    test_df
  )
})

test_that("extract yday from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = yday(datetime)) %>%
      collect(),
    test_df
  )
})

test_that("extract hour from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = hour(datetime)) %>%
      collect(),
    test_df
  )
})

test_that("extract minute from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = minute(datetime)) %>%
      collect(),
    test_df
  )
})

test_that("extract second from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = second(datetime)) %>%
      collect(),
    test_df,
    # arrow supports nanosecond resolution but lubridate does not
    tolerance = 1e-6
  )
})

# These tests test extraction of components from date32 objects

test_that("extract year from date", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = year(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract isoyear from date", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = isoyear(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract quarter from date", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = quarter(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract month from date", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = month(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract isoweek from date", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = isoweek(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract epiweek from date", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = epiweek(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract day from date", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = day(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract wday from date", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = wday(date)) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = wday(date, week_start = 3)) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = wday(date, week_start = 1)) %>%
      collect(),
    test_df
  )

  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-13168

  compare_dplyr_binding(
    .input %>%
      mutate(x = wday(date, label = TRUE, abbr = TRUE)) %>%
      mutate(x = as.character(x)) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = wday(date, label = TRUE)) %>%
      mutate(x = as.character(x)) %>%
      collect(),
    test_df
  )
})

test_that("extract yday from date", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = yday(date)) %>%
      collect(),
    test_df
  )
})
