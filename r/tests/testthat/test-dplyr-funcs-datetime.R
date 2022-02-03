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
  date = c(as.Date("2021-09-09"), NA),
  integer = 1:2
)


test_that("strptime", {
  t_string <- tibble(x = c("2018-10-07 19:04:05", NA))
  t_stamp <- tibble(x = c(lubridate::ymd_hms("2018-10-07 19:04:05"), NA))

  expect_equal(
    t_string %>%
      Table$create() %>%
      mutate(
        x = strptime(x)
      ) %>%
      collect(),
    t_stamp,
    ignore_attr = "tzone"
  )

  expect_equal(
    t_string %>%
      Table$create() %>%
      mutate(
        x = strptime(x, format = "%Y-%m-%d %H:%M:%S")
      ) %>%
      collect(),
    t_stamp,
    ignore_attr = "tzone"
  )

  expect_equal(
    t_string %>%
      Table$create() %>%
      mutate(
        x = strptime(x, format = "%Y-%m-%d %H:%M:%S", unit = "ns")
      ) %>%
      collect(),
    t_stamp,
    ignore_attr = "tzone"
  )

  expect_equal(
    t_string %>%
      Table$create() %>%
      mutate(
        x = strptime(x, format = "%Y-%m-%d %H:%M:%S", unit = "s")
      ) %>%
      collect(),
    t_stamp,
    ignore_attr = "tzone"
  )

  tstring <- tibble(x = c("08-05-2008", NA))
  tstamp <- strptime(c("08-05-2008", NA), format = "%m-%d-%Y")

  expect_equal(
    tstring %>%
      Table$create() %>%
      mutate(
        x = strptime(x, format = "%m-%d-%Y")
      ) %>%
      pull(),
    # R's strptime returns POSIXlt (list type)
    as.POSIXct(tstamp),
    ignore_attr = "tzone"
  )
})

test_that("errors in strptime", {
  # Error when tz is passed
  x <- Expression$field_ref("x")
  expect_error(
    call_binding("strptime", x, tz = "PDT"),
    "Time zone argument not supported in Arrow"
  )
})

test_that("strftime", {
  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-13168

  times <- tibble(
    datetime = c(lubridate::ymd_hms("2018-10-07 19:04:05", tz = "Etc/GMT+6"), NA),
    date = c(as.Date("2021-01-01"), NA)
  )
  formats <- "%a %A %w %d %b %B %m %y %Y %H %I %p %M %z %Z %j %U %W %x %X %% %G %V %u"
  formats_date <- "%a %A %w %d %b %B %m %y %Y %H %I %p %M %j %U %W %x %X %% %G %V %u"

  compare_dplyr_binding(
    .input %>%
      mutate(x = strftime(datetime, format = formats)) %>%
      collect(),
    times
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = strftime(date, format = formats_date)) %>%
      collect(),
    times
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = strftime(datetime, format = formats, tz = "Pacific/Marquesas")) %>%
      collect(),
    times
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = strftime(datetime, format = formats, tz = "EST", usetz = TRUE)) %>%
      collect(),
    times
  )

  withr::with_timezone(
    "Pacific/Marquesas",
    {
      compare_dplyr_binding(
        .input %>%
          mutate(
            x = strftime(datetime, format = formats, tz = "EST"),
            x_date = strftime(date, format = formats_date, tz = "EST")
          ) %>%
          collect(),
        times
      )

      compare_dplyr_binding(
        .input %>%
          mutate(
            x = strftime(datetime, format = formats),
            x_date = strftime(date, format = formats_date)
          ) %>%
          collect(),
        times
      )
    }
  )

  # This check is due to differences in the way %c currently works in Arrow and R's strftime.
  # We can revisit after https://github.com/HowardHinnant/date/issues/704 is resolved.
  expect_error(
    times %>%
      Table$create() %>%
      mutate(x = strftime(datetime, format = "%c")) %>%
      collect(),
    "%c flag is not supported in non-C locales."
  )

  # Output precision of %S depends on the input timestamp precision.
  # Timestamps with second precision are represented as integers while
  # milliseconds, microsecond and nanoseconds are represented as fixed floating
  # point numbers with 3, 6 and 9 decimal places respectively.
  compare_dplyr_binding(
    .input %>%
      mutate(x = strftime(datetime, format = "%S")) %>%
      transmute(as.double(substr(x, 1, 2))) %>%
      collect(),
    times,
    tolerance = 1e-6
  )
})

test_that("format_ISO8601", {
  # https://issues.apache.org/jira/projects/ARROW/issues/ARROW-15266
  skip_if_not_available("re2")
  # https://issues.apache.org/jira/browse/ARROW-13168
  skip_on_os("windows")
  times <- tibble(x = c(lubridate::ymd_hms("2018-10-07 19:04:05", tz = "Etc/GMT+6"), NA))

  compare_dplyr_binding(
    .input %>%
      mutate(x = format_ISO8601(x, precision = "ymd", usetz = FALSE)) %>%
      collect(),
    times
  )

  if (getRversion() < "3.5") {
    # before 3.5, times$x will have no timezone attribute, so Arrow faithfully
    # errors that there is no timezone to format:
    expect_error(
      times %>%
        Table$create() %>%
        mutate(x = format_ISO8601(x, precision = "ymd", usetz = TRUE)) %>%
        collect(),
      "Timezone not present, cannot convert to string with timezone: %Y-%m-%d%z"
    )

    # See comment regarding %S flag in strftime tests
    expect_error(
      times %>%
        Table$create() %>%
        mutate(x = format_ISO8601(x, precision = "ymdhms", usetz = TRUE)) %>%
        mutate(x = gsub("\\.0*", "", x)) %>%
        collect(),
      "Timezone not present, cannot convert to string with timezone: %Y-%m-%dT%H:%M:%S%z"
    )
  } else {
    compare_dplyr_binding(
      .input %>%
        mutate(x = format_ISO8601(x, precision = "ymd", usetz = TRUE)) %>%
        collect(),
      times
    )

    # See comment regarding %S flag in strftime tests
    compare_dplyr_binding(
      .input %>%
        mutate(x = format_ISO8601(x, precision = "ymdhms", usetz = TRUE)) %>%
        mutate(x = gsub("\\.0*", "", x)) %>%
        collect(),
      times
    )
  }


  # See comment regarding %S flag in strftime tests
  compare_dplyr_binding(
    .input %>%
      mutate(x = format_ISO8601(x, precision = "ymdhms", usetz = FALSE)) %>%
      mutate(x = gsub("\\.0*", "", x)) %>%
      collect(),
    times
  )
})

# These tests test detection of dates and times

test_that("is.* functions from lubridate", {
  # make sure all true and at least one false value is considered
  compare_dplyr_binding(
    .input %>%
      mutate(x = is.POSIXct(datetime), y = is.POSIXct(integer)) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = is.Date(date), y = is.Date(integer)) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        x = is.instant(datetime),
        y = is.instant(date),
        z = is.instant(integer)
      ) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        x = is.timepoint(datetime),
        y = is.instant(date),
        z = is.timepoint(integer)
      ) %>%
      collect(),
    test_df
  )
})

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

test_that("extract epiyear from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = epiyear(datetime)) %>%
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

  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-13168

  compare_dplyr_binding(
    .input %>%
      # R returns ordered factor whereas Arrow returns character
      mutate(x = as.character(month(datetime, label = TRUE))) %>%
      collect(),
    test_df,
    ignore_attr = TRUE
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = as.character(month(datetime, label = TRUE, abbr = TRUE))) %>%
      collect(),
    test_df,
    ignore_attr = TRUE
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

test_that("extract week from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = week(datetime)) %>%
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

test_that("extract mday from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = mday(datetime)) %>%
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

test_that("extract epiyear from date", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = epiyear(date)) %>%
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

test_that("extract week from date", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = week(date)) %>%
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

  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-13168

  compare_dplyr_binding(
    .input %>%
      # R returns ordered factor whereas Arrow returns character
      mutate(x = as.character(month(date, label = TRUE))) %>%
      collect(),
    test_df,
    ignore_attr = TRUE
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = as.character(month(date, label = TRUE, abbr = TRUE))) %>%
      collect(),
    test_df,
    ignore_attr = TRUE
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

test_that("extract mday from date", {
  compare_dplyr_binding(
    .input %>%
      mutate(x = mday(date)) %>%
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

test_that("leap_year mirror lubridate", {

  compare_dplyr_binding(
    .input %>%
      mutate(x = leap_year(date)) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = leap_year(datetime)) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = leap_year(test_year)) %>%
      collect(),
    data.frame(
      test_year = as.Date(c(
        "1998-01-01", # not leap year
        "1996-01-01", # leap year (divide by 4 rule)
        "1900-01-01", # not leap year (divide by 100 rule)
        "2000-01-01"  # leap year (divide by 400 rule)
      ))
    )
  )

})

test_that("am/pm mirror lubridate", {

  # https://issues.apache.org/jira/browse/ARROW-13168
  skip_on_os("windows")

  compare_dplyr_binding(
    .input %>%
      mutate(
        am = am(test_time),
        pm = pm(test_time)
      ) %>%
      collect(),
    data.frame(
      test_time = strptime(
        x = c(
          "2022-01-25 11:50:59",
          "2022-01-25 12:00:00",
          "2022-01-25 00:00:00"
        ),
        format = "%Y-%m-%d %H:%M:%S"
      )

    )
  )

})

test_that("extract tz", {
  df <- tibble(
    posixct_date = as.POSIXct(c("2022-02-07", "2022-02-10"), tz = "Pacific/Marquesas"),
  )

  compare_dplyr_binding(
    .input %>%
      mutate(timezone_posixct_date = tz(posixct_date)) %>%
      collect(),
    df
  )

  # test a few types directly from R objects
  expect_error(
    call_binding("tz", "2020-10-01"),
    "timezone extraction for objects of class `string` not supported in Arrow"
  )
  expect_error(
    call_binding("tz", as.Date("2020-10-01")),
    "timezone extraction for objects of class `date32[day]` not supported in Arrow",
    fixed = TRUE
  )
  expect_error(
    call_binding("tz", 1L),
    "timezone extraction for objects of class `int32` not supported in Arrow"
  )
   expect_error(
    call_binding("tz", 1.1),
    "timezone extraction for objects of class `double` not supported in Arrow"
  )

  # Test one expression
   expect_error(
     call_binding("tz", Expression$scalar("2020-10-01")),
     "timezone extraction for objects of class `string` not supported in Arrow"
   )
})

test_that("semester works with temporal types and integers", {
  test_df <- tibble(
    month_as_int = c(1:12, NA),
    month_as_char_pad = sprintf("%02i", month_as_int),
    dates = as.Date(paste0("2021-", month_as_char_pad, "-15"))
  )

  # semester extraction from dates
  compare_dplyr_binding(
     .input %>%
      mutate(sem_wo_year = semester(dates),
             sem_w_year = semester(dates, with_year = TRUE)) %>%
      collect(),
     test_df
  )
  # semester extraction from months as integers is not supported yet
  # it will be once https://issues.apache.org/jira/browse/ARROW-15701 is done
  # TODO change from expect_error to compare_dplyr_bindings
  expect_error(
    test_df %>%
      arrow_table() %>%
      mutate(sem_month_as_int = semester(month_as_int)) %>%
      collect(),
    regexp = "NotImplemented: Function 'month' has no kernel matching input types (array[int32])",
    fixed = TRUE
  )

  expect_error(
    test_df %>%
      arrow_table() %>%
      mutate(sem_month_as_char_pad = semester(month_as_char_pad)) %>%
      collect(),
    regexp = "NotImplemented: Function 'month' has no kernel matching input types (array[string])",
    fixed = TRUE
    )
  })

test_that("dst extracts daylight savings time correctly", {
  test_df <- tibble(
    dates = as.POSIXct(c("2021-02-20", "2021-07-31", "2021-10-31", "2021-01-31"), tz = "Europe/London")
  )
  # https://issues.apache.org/jira/browse/ARROW-13168
  skip_on_os("windows")

  compare_dplyr_binding(
    .input %>%
      mutate(dst = dst(dates)) %>%
      collect(),
    test_df
  )
})

test_that("date works in arrow", {
  # https://issues.apache.org/jira/browse/ARROW-13168
  skip_on_os("windows")
  # this date is specific since lubridate::date() is different from base::as.Date()
  # since as.Date returns the UTC date and date() doesn't
  test_df <- tibble(
    posixct_date = as.POSIXct(c("2012-03-26 23:12:13", NA), tz = "America/New_York"),
    integer_var = c(32L, NA))

  r_date_object <- lubridate::ymd_hms("2012-03-26 23:12:13")

  # we can't (for now) use namespacing, so we need to make sure lubridate::date()
  # and not base::date() is being used. This is due to the way testthat runs and
  # normal use of arrow would not have to do this explicitly.
  # TODO remove once https://issues.apache.org/jira/browse/ARROW-14575 is done
  date <- lubridate::date

  compare_dplyr_binding(
    .input %>%
      mutate(a_date = date(posixct_date)) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(a_date_base = as.Date(posixct_date)) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(date_from_r_object = date(r_date_object)) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(as_date_from_r_object = as.Date(r_date_object)) %>%
      collect(),
    test_df
  )

  # date from integer supported in arrow (similar to base::as.Date()), but in
  # Arrow it assumes a fixed origin "1970-01-01". However this is not supported
  # by lubridate. lubridate::date(integer_var) errors without an `origin`
  expect_equal(
    test_df %>%
      arrow_table() %>%
      select(integer_var) %>%
      mutate(date_int = date(integer_var)) %>%
      collect(),
    tibble(integer_var = c(32L, NA),
           date_int = as.Date(c("1970-02-02", NA)))
  )
})

test_that("date() errors with unsupported inputs", {
  expect_error(
    example_data %>%
      arrow_table() %>%
      mutate(date_char = date("2022-02-25 00:00:01")) %>%
      collect(),
    regexp = "Unsupported cast from string to date32 using function cast_date32"
  )

  expect_error(
    example_data %>%
      arrow_table() %>%
      mutate(date_bool = date(TRUE)) %>%
      collect(),
    regexp = "Unsupported cast from bool to date32 using function cast_date32"
  )

  expect_error(
    example_data %>%
      arrow_table() %>%
      mutate(date_double = date(34.56)) %>%
      collect(),
    regexp = "Unsupported cast from double to date32 using function cast_date32"
  )
})
