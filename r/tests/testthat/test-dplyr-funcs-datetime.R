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
# In 3.4 the lack of tzone attribute causes spurious failures
skip_on_r_older_than("3.5")

library(lubridate, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)

# base::strptime() defaults to local timezone
# but arrow's strptime defaults to UTC.
# So that tests are consistent, set the local timezone to UTC
# TODO: consider reevaluating now that ARROW-12980 has merged
withr::local_timezone("UTC")

if (tolower(Sys.info()[["sysname"]]) == "windows") {
  withr::local_locale(LC_TIME = "C")
}

test_date <- as.POSIXct("2017-01-01 00:00:11.3456789", tz = "Pacific/Marquesas")


test_df <- tibble::tibble(
  # test_date + 1 turns the tzone = "" to NULL, which is functionally equivalent
  # so we can run some tests on Windows, but this skirts around ARROW-13588.
  # That issue is tough because in C++, "" is the "no timezone" value
  # due to static typing, so we can't distinguish a literal "" from NULL
  datetime = c(test_date, NA) + 1,
  date = c(as.Date("2021-09-09"), NA),
  integer = 1:2
)


test_that("strptime", {
  t_string <- tibble(x = c("2018-10-07 19:04:05", NA))
  # lubridate defaults to "UTC" as timezone => t_stamp is in "UTC"
  t_stamp_with_utc_tz <- tibble(x = c(lubridate::ymd_hms("2018-10-07 19:04:05"), NA))
  t_stamp_with_pm_tz <- tibble(
    x = c(lubridate::ymd_hms("2018-10-07 19:04:05", tz = "Pacific/Marquesas"), NA)
  )

  # base::strptime returns a POSIXlt (a list) => we cannot use compare_dplyr_binding
  # => we use expect_equal for the tests below

  withr::with_timezone("Pacific/Marquesas", {
    # the default value for strptime's `tz` argument is "", which is interpreted
    # as the current timezone. we test here if the strptime binding picks up
    # correctly the current timezone (similarly to the base R version)
    expect_equal(
      t_string %>%
        record_batch() %>%
        mutate(
          x = strptime(x, format = "%Y-%m-%d %H:%M:%S")
        ) %>%
        collect(),
      t_stamp_with_pm_tz
    )

    expect_equal(
      t_string %>%
        record_batch() %>%
        mutate(
          x = base::strptime(x, format = "%Y-%m-%d %H:%M:%S")
        ) %>%
        collect(),
      t_stamp_with_pm_tz
    )
  })

  # adding a timezone to a timezone-naive timestamp works
  # and since our TZ when running the test is (typically) Pacific/Marquesas
  # this also tests that assigning a TZ different from the current session one
  # works as expected
  expect_equal(
    t_string %>%
      arrow_table() %>%
      mutate(
        x = strptime(x, format = "%Y-%m-%d %H:%M:%S", tz = "Pacific/Marquesas")
      ) %>%
      collect(),
    t_stamp_with_pm_tz
  )

  expect_equal(
    t_string %>%
      Table$create() %>%
      mutate(
        x = strptime(x, tz = "UTC")
      ) %>%
      collect(),
    t_stamp_with_utc_tz
  )

  expect_equal(
    t_string %>%
      Table$create() %>%
      mutate(
        x = strptime(x, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
      ) %>%
      collect(),
    t_stamp_with_utc_tz
  )

  expect_equal(
    t_string %>%
      Table$create() %>%
      mutate(
        x = strptime(x, format = "%Y-%m-%d %H:%M:%S", unit = "ns", tz = "UTC")
      ) %>%
      collect(),
    t_stamp_with_utc_tz
  )

  expect_equal(
    t_string %>%
      Table$create() %>%
      mutate(
        x = strptime(x, format = "%Y-%m-%d %H:%M:%S", unit = "s", tz = "UTC")
      ) %>%
      collect(),
    t_stamp_with_utc_tz
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

test_that("strptime returns NA when format doesn't match the data", {
  df <- tibble(
    str_date = c("2022-02-07", "2012/02-07", "1975/01-02", "1981/01-07", NA)
  )

  # base::strptime() returns a POSIXlt object (a list), while the Arrow binding
  # returns a POSIXct (double) vector => we cannot use compare_dplyr_binding()
  expect_equal(
    df %>%
      arrow_table() %>%
      mutate(
        r_obj_parsed_date = strptime("03-27/2022", format = "%m-%d/%Y"),
        r_obj_parsed_na = strptime("03-27/2022", format = "Y%-%m-%d")
      ) %>%
      collect(),
    df %>%
      mutate(
        r_obj_parsed_date = as.POSIXct(strptime("03-27/2022", format = "%m-%d/%Y")),
        r_obj_parsed_na = as.POSIXct(strptime("03-27/2022", format = "Y%-%m-%d"))
      ),
    ignore_attr = "tzone"
  )

  expect_equal(
    df %>%
      record_batch() %>%
      mutate(parsed_date = strptime(str_date, format = "%Y-%m-%d")) %>%
      collect(),
    df %>%
      mutate(parsed_date = as.POSIXct(strptime(str_date, format = "%Y-%m-%d"))),
    ignore_attr = "tzone"
  )

  expect_equal(
    df %>%
      arrow_table() %>%
      mutate(parsed_date = strptime(str_date, format = "%Y/%m-%d")) %>%
      collect(),
    df %>%
      mutate(parsed_date = as.POSIXct(strptime(str_date, format = "%Y/%m-%d"))),
    ignore_attr = "tzone"
  )
})

test_that("strftime", {
  times <- tibble(
    datetime = c(lubridate::ymd_hms("2018-10-07 19:04:05", tz = "Etc/GMT+6"), NA),
    date = c(as.Date("2021-01-01"), NA)
  )
  formats <- "%a %A %w %d %b %B %m %y %Y %H %I %p %M %z %Z %j %U %W %x %X %% %G %V %u"
  formats_date <- "%a %A %w %d %b %B %m %y %Y %H %I %p %M %j %U %W %x %X %% %G %V %u"

  compare_dplyr_binding(
    .input %>%
      mutate(
        x = strftime(datetime, format = formats),
        x2 = base::strftime(datetime, format = formats)
      ) %>%
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
  if (Sys.getlocale("LC_TIME") != "C") {
    expect_error(
      times %>%
        Table$create() %>%
        mutate(x = strftime(datetime, format = "%c")) %>%
        collect(),
      "%c flag is not supported in non-C locales."
    )
  }

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
  times <- tibble(x = c(lubridate::ymd_hms("2018-10-07 19:04:05", tz = "Etc/GMT+6"), NA))

  compare_dplyr_binding(
    .input %>%
      mutate(
        a = format_ISO8601(x, precision = "ymd", usetz = FALSE),
        a2 = lubridate::format_ISO8601(x, precision = "ymd", usetz = FALSE)
      ) %>%
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
      mutate(
        x = is.POSIXct(datetime),
        y = is.POSIXct(integer),
        x2 = lubridate::is.POSIXct(datetime)
      ) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        x = is.Date(date),
        y = is.Date(integer),
        x2 = lubridate::is.Date(date)
      ) %>%
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
        z = is.timepoint(integer),
        x2 = lubridate::is.timepoint(datetime),
        y2 = lubridate::is.instant(date),
        z2 = lubridate::is.timepoint(integer)
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
      mutate(
        x = epiyear(datetime),
        x2 = lubridate::epiyear(datetime)
      ) %>%
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
      mutate(
        x = month(datetime),
        x2 = lubridate::month(datetime)
      ) %>%
      collect(),
    test_df
  )

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
      mutate(
        x = isoweek(datetime),
        x2 = lubridate::isoweek(datetime)
      ) %>%
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
      mutate(
        x = week(datetime),
        x2 = lubridate::week(datetime)
      ) %>%
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
      mutate(
        x = yday(datetime),
        x2 = lubridate::yday(datetime)
      ) %>%
      collect(),
    test_df
  )
})

test_that("extract hour from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(
        x = hour(datetime),
        x2 = lubridate::hour(datetime)
      ) %>%
      collect(),
    test_df
  )
})

test_that("extract minute from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(
        x = minute(datetime),
        x2 = lubridate::minute(datetime)
      ) %>%
      collect(),
    test_df
  )
})

test_that("extract second from timestamp", {
  compare_dplyr_binding(
    .input %>%
      mutate(
        x = second(datetime),
        x2 = lubridate::second(datetime)
      ) %>%
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
      mutate(
        x = year(date),
        x2 = lubridate::year(date)
      ) %>%
      collect(),
    test_df
  )
})

test_that("extract isoyear from date", {
  compare_dplyr_binding(
    .input %>%
      mutate(
        x = isoyear(date),
        x2 = lubridate::isoyear(date)
      ) %>%
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
      mutate(
        x = quarter(date),
        x2 = lubridate::quarter(date)
      ) %>%
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
      mutate(
        x = epiweek(date),
        x2 = lubridate::epiweek(date)
      ) %>%
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
      mutate(
        x = day(date),
        x2 = lubridate::day(date)
      ) %>%
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
      mutate(
        x = wday(date, week_start = 3),
        x2 = lubridate::wday(date, week_start = 3)
      ) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = wday(date, week_start = 1)) %>%
      collect(),
    test_df
  )

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
      mutate(
        x = mday(date),
        x2 = lubridate::mday(date)
      ) %>%
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
      mutate(
        x = leap_year(date),
        x2 = lubridate::leap_year(date)
      ) %>%
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
        "2000-01-01" # leap year (divide by 400 rule)
      ))
    )
  )
})

test_that("am/pm mirror lubridate", {
  compare_dplyr_binding(
    .input %>%
      mutate(
        am = am(test_time),
        pm = pm(test_time),
        am2 = lubridate::am(test_time),
        pm2 = lubridate::pm(test_time)
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
      mutate(
        timezone_posixct_date = tz(posixct_date),
        timezone_posixct_date2 = lubridate::tz(posixct_date)
      ) %>%
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
      mutate(
        sem_wo_year = semester(dates),
        sem_wo_year2 = lubridate::semester(dates),
        sem_w_year = semester(dates, with_year = TRUE)
      ) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(sem_month_as_int = semester(month_as_int)) %>%
      collect(),
    test_df
  )

  expect_error(
    test_df %>%
      arrow_table() %>%
      mutate(sem_month_as_char_pad = semester(month_as_char_pad)) %>%
      collect(),
    regexp = "NotImplemented: Function 'month' has no kernel matching input types (string)",
    fixed = TRUE
  )
})

test_that("dst extracts daylight savings time correctly", {
  test_df <- tibble(
    dates = as.POSIXct(c("2021-02-20", "2021-07-31", "2021-10-31", "2021-01-31"), tz = "Europe/London")
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        dst = dst(dates),
        dst2 = lubridate::dst(dates)
      ) %>%
      collect(),
    test_df
  )
})

test_that("month() supports integer input", {
  test_df_month <- tibble(
    month_as_int = c(1:12, NA)
  )

  compare_dplyr_binding(
    .input %>%
      mutate(month_int_input = month(month_as_int)) %>%
      collect(),
    test_df_month
  )

  compare_dplyr_binding(
    .input %>%
      # R returns ordered factor whereas Arrow returns character
      mutate(
        month_int_input = as.character(month(month_as_int, label = TRUE))
      ) %>%
      collect(),
    test_df_month
  )

  compare_dplyr_binding(
    .input %>%
      # R returns ordered factor whereas Arrow returns character
      mutate(
        month_int_input = as.character(
          month(month_as_int, label = TRUE, abbr = FALSE)
        )
      ) %>%
      collect(),
    test_df_month
  )
})

test_that("month() errors with double input and returns NA with int outside 1:12", {
  test_df_month <- tibble(
    month_as_int = c(-1L, 1L, 13L, NA),
    month_as_double = month_as_int + 0.1
  )

  expect_equal(
    test_df_month %>%
      arrow_table() %>%
      select(month_as_int) %>%
      mutate(month_int_input = month(month_as_int)) %>%
      collect(),
    tibble(
      month_as_int = c(-1L, 1L, 13L, NA),
      month_int_input = c(NA, 1L, NA, NA)
    )
  )

  expect_error(
    test_df_month %>%
      arrow_table() %>%
      mutate(month_dbl_input = month(month_as_double)) %>%
      collect(),
    regexp = "Function 'month' has no kernel matching input types (double)",
    fixed = TRUE
  )

  expect_error(
    test_df_month %>%
      record_batch() %>%
      mutate(month_dbl_input = month(month_as_double)) %>%
      collect(),
    regexp = "Function 'month' has no kernel matching input types (double)",
    fixed = TRUE
  )
})

test_that("date works in arrow", {
  # this date is specific since lubridate::date() is different from base::as.Date()
  # since as.Date returns the UTC date and date() doesn't
  test_df <- tibble(
    posixct_date = as.POSIXct(c("2012-03-26 23:12:13", NA), tz = "America/New_York"),
    integer_var = c(32L, NA)
  )

  r_date_object <- lubridate::ymd_hms("2012-03-26 23:12:13")

  compare_dplyr_binding(
    .input %>%
      mutate(a_date = lubridate::date(posixct_date)) %>%
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
      mutate(date_from_r_object = lubridate::date(r_date_object)) %>%
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
    tibble(
      integer_var = c(32L, NA),
      date_int = as.Date(c("1970-02-02", NA))
    )
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

test_that("make_date & make_datetime", {
  test_df <- expand.grid(
    year = c(1999, 1969, 2069, NA),
    month = c(1, 2, 7, 12, NA),
    day = c(1, 9, 13, 28, NA),
    hour = c(0, 7, 23, NA),
    min = c(0, 59, NA),
    sec = c(0, 59, NA)
  ) %>%
    tibble()

  compare_dplyr_binding(
    .input %>%
      mutate(
        composed_date = make_date(year, month, day),
        composed_date2 = lubridate::make_date(year, month, day)
      ) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(composed_date_r_obj = make_date(1999, 12, 31)) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        composed_datetime = make_datetime(year, month, day, hour, min, sec),
        composed_datetime2 = lubridate::make_datetime(year, month, day, hour, min, sec)
      ) %>%
      collect(),
    test_df,
    # the make_datetime binding uses strptime which does not support tz, hence
    # a mismatch in tzone attribute (ARROW-12820)
    ignore_attr = TRUE
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        composed_datetime_r_obj = make_datetime(1999, 12, 31, 14, 15, 16)
      ) %>%
      collect(),
    test_df,
    # the make_datetime binding uses strptime which does not support tz, hence
    # a mismatch in tzone attribute (ARROW-12820)
    ignore_attr = TRUE
  )
})

test_that("ISO_datetime & ISOdate", {
  test_df <- expand.grid(
    year = c(1999, 1969, 2069, NA),
    month = c(1, 2, 7, 12, NA),
    day = c(1, 9, 13, 28, NA),
    hour = c(0, 7, 23, NA),
    min = c(0, 59, NA),
    sec = c(0, 59, NA)
  ) %>%
    tibble()

  compare_dplyr_binding(
    .input %>%
      mutate(
        composed_date = ISOdate(year, month, day),
        composed_date2 = base::ISOdate(year, month, day)
      ) %>%
      collect(),
    test_df,
    # the make_datetime binding uses strptime which does not support tz, hence
    # a mismatch in tzone attribute (ARROW-12820)
    ignore_attr = TRUE
  )

  compare_dplyr_binding(
    .input %>%
      mutate(composed_date_r_obj = ISOdate(1999, 12, 31)) %>%
      collect(),
    test_df,
    # the make_datetime binding uses strptime which does not support tz, hence
    # a mismatch in tzone attribute (ARROW-12820)
    ignore_attr = TRUE
  )

  # the default `tz` for base::ISOdatetime is "", but in Arrow it's "UTC"
  compare_dplyr_binding(
    .input %>%
      mutate(
        composed_datetime = ISOdatetime(year, month, day, hour, min, sec, tz = "UTC"),
        composed_datetime2 = base::ISOdatetime(year, month, day, hour, min, sec, tz = "UTC")
      ) %>%
      collect(),
    test_df,
    # the make_datetime binding uses strptime which does not support tz, hence
    # a mismatch in tzone attribute (ARROW-12820)
    ignore_attr = TRUE
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        composed_datetime_r_obj = ISOdatetime(1999, 12, 31, 14, 15, 16)
      ) %>%
      collect(),
    test_df,
    # the make_datetime binding uses strptime which does not support tz, hence
    # a mismatch in tzone attribute (ARROW-12820)
    ignore_attr = TRUE
  )
})

test_that("difftime()", {
  test_df <- tibble(
    time1 = as.POSIXct(
      c("2021-02-20", "2021-07-31 0:0:0", "2021-10-30", "2021-01-31 0:0:0")
    ),
    time2 = as.POSIXct(
      c("2021-02-20 00:02:01", "2021-07-31 00:03:54", "2021-10-30 00:05:45", "2021-01-31 00:07:36")
    ),
    secs = c(121L, 234L, 345L, 456L)
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        secs = difftime(time1, time2, units = "secs"),
        secs2 = base::difftime(time1, time2, units = "secs")
      ) %>%
      collect(),
    test_df,
    ignore_attr = TRUE
  )

  # units other than "secs" not supported in arrow
  compare_dplyr_binding(
    .input %>%
      mutate(
        mins = difftime(time1, time2, units = "mins")
      ) %>%
      collect(),
    test_df,
    warning = TRUE,
    ignore_attr = TRUE
  )

  test_df_with_tz <- tibble(
    time1 = as.POSIXct(
      c("2021-02-20", "2021-07-31", "2021-10-30", "2021-01-31"),
      tz = "Pacific/Marquesas"
    ),
    time2 = as.POSIXct(
      c("2021-02-20 00:02:01", "2021-07-31 00:03:54", "2021-10-30 00:05:45", "2021-01-31 00:07:36"),
      tz = "Asia/Kathmandu"
    ),
    secs = c(121L, 234L, 345L, 456L)
  )

  compare_dplyr_binding(
    .input %>%
      mutate(secs2 = difftime(time2, time1, units = "secs")) %>%
      collect(),
    test_df_with_tz
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        secs2 = difftime(
          as.POSIXct("2022-03-07", tz = "Pacific/Marquesas"),
          time1,
          units = "secs"
        )
      ) %>%
      collect(),
    test_df_with_tz
  )

  # `tz` is effectively ignored both in R (used only if inputs are POSIXlt) and Arrow
  compare_dplyr_binding(
    .input %>%
      mutate(secs2 = difftime(time2, time1, units = "secs", tz = "Pacific/Marquesas")) %>%
      collect(),
    test_df_with_tz,
    warning = "`tz` argument is not supported in Arrow, so it will be ignored"
  )
})

test_that("as.difftime()", {
  test_df <- tibble(
    hms_string = c("0:7:45", "12:34:56"),
    hm_string = c("7:45", "12:34"),
    int = c(30L, 75L),
    integerish_dbl = c(31, 76),
    dbl = c(31.2, 76.4)
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        hms_difftime = as.difftime(hms_string, units = "secs"),
        hms_difftime2 = base::as.difftime(hms_string, units = "secs")
      ) %>%
      collect(),
    test_df
  )

  # TODO add test with `format` mismatch returning NA once
  # https://issues.apache.org/jira/browse/ARROW-15659 is solved
  # for example: as.difftime("07:", format = "%H:%M") should return NA
  compare_dplyr_binding(
    .input %>%
      mutate(hm_difftime = as.difftime(hm_string, units = "secs", format = "%H:%M")) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(int_difftime = as.difftime(int, units = "secs")) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(integerish_dbl_difftime = as.difftime(integerish_dbl, units = "secs")) %>%
      collect(),
    test_df
  )

  # "mins" or other values for units cannot be handled in Arrow
  compare_dplyr_binding(
    .input %>%
      mutate(int_difftime = as.difftime(int, units = "mins")) %>%
      collect(),
    test_df,
    warning = TRUE
  )

  # only integer (or integer-like) -> duration conversion supported in Arrow.
  # double -> duration not supported. we're not testing the content of the
  # error message as it is being generated in the C++ code and it might change,
  # but we want to make sure that this error is raised in our binding implementation
  expect_error(
    test_df %>%
      arrow_table() %>%
      mutate(dbl_difftime = as.difftime(dbl, units = "secs")) %>%
      collect()
  )
})

test_that("`decimal_date()` and `date_decimal()`", {
  test_df <- tibble(
    a = c(
      2007.38998954347, 1970.77732069883, 2020.96061799722,
      2009.43465948477, 1975.71251467871, NA
    ),
    b = as.POSIXct(
      c(
        "2007-05-23 08:18:30", "1970-10-11 17:19:45", "2020-12-17 14:04:06",
        "2009-06-08 15:37:01", "1975-09-18 01:37:42", NA
      )
    ),
    c = as.Date(
      c("2007-05-23", "1970-10-11", "2020-12-17", "2009-06-08", "1975-09-18", NA)
    )
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        decimal_date_from_POSIXct = decimal_date(b),
        decimal_date_from_POSIXct2 = lubridate::decimal_date(b),
        decimal_date_from_r_POSIXct_obj = decimal_date(as.POSIXct("2022-03-25 15:37:01")),
        decimal_date_from_r_date_obj = decimal_date(as.Date("2022-03-25")),
        decimal_date_from_date = decimal_date(c),
        date_from_decimal = date_decimal(a),
        date_from_decimal2 = lubridate::date_decimal(a),
        date_from_decimal_r_obj = date_decimal(2022.178)
      ) %>%
      collect(),
    test_df,
    ignore_attr = "tzone"
  )
})

test_that("dminutes, dhours, ddays, dweeks, dmonths, dyears", {
  example_d <- tibble(x = c(1:10, NA))
  date_to_add <- ymd("2009-08-03", tz = "Pacific/Marquesas")

  # When comparing results we use ignore_attr = TRUE because of the diff in:
  # attribute 'package' (absent vs. 'lubridate')
  # class (difftime vs Duration)
  # attribute 'units' (character vector ('secs') vs. absent)

  compare_dplyr_binding(
    .input %>%
      mutate(
        dminutes = dminutes(x),
        dhours = dhours(x),
        ddays = ddays(x),
        dweeks = dweeks(x),
        dmonths = dmonths(x),
        dyears = dyears(x)
      ) %>%
      collect(),
    example_d,
    ignore_attr = TRUE
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        dhours = dhours(x),
        ddays = ddays(x),
        new_date_1 = date_to_add + ddays,
        new_date_2 = date_to_add + ddays - dhours(3),
        new_duration = dhours - ddays
      ) %>%
      collect(),
    example_d,
    ignore_attr = TRUE
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        r_obj_dminutes = dminutes(1),
        r_obj_dhours = dhours(2),
        r_obj_ddays = ddays(3),
        r_obj_dweeks = dweeks(4),
        r_obj_dmonths = dmonths(5),
        r_obj_dyears = dyears(6),
        r_obj_dminutes2 = lubridate::dminutes(1),
        r_obj_dhours2 = lubridate::dhours(2),
        r_obj_ddays2 = lubridate::ddays(3),
        r_obj_dweeks2 = lubridate::dweeks(4),
        r_obj_dmonths2 = lubridate::dmonths(5),
        r_obj_dyears2 = lubridate::dyears(6)
      ) %>%
      collect(),
    tibble(),
    ignore_attr = TRUE
  )

  # double -> duration not supported in Arrow.
  # Error is generated in the C++ code
  expect_error(
    test_df %>%
      arrow_table() %>%
      mutate(r_obj_dminutes = dminutes(1.12345)) %>%
      collect()
  )
})

test_that("dseconds, dmilliseconds, dmicroseconds, dnanoseconds, dpicoseconds", {
  example_d <- tibble(x = c(1:10, NA))
  date_to_add <- ymd("2009-08-03", tz = "Pacific/Marquesas")

  # When comparing results we use ignore_attr = TRUE because of the diff in:
  # attribute 'package' (absent vs. 'lubridate')
  # class (difftime vs Duration)
  # attribute 'units' (character vector ('secs') vs. absent)

  compare_dplyr_binding(
    .input %>%
      mutate(
        dseconds = dseconds(x),
        dmilliseconds = dmilliseconds(x),
        dmicroseconds = dmicroseconds(x),
        dnanoseconds = dnanoseconds(x),
        dseconds2 = lubridate::dseconds(x),
        dmilliseconds2 = lubridate::dmilliseconds(x),
        dmicroseconds2 = lubridate::dmicroseconds(x),
        dnanoseconds2 = lubridate::dnanoseconds(x),
      ) %>%
      collect(),
    example_d,
    ignore_attr = TRUE
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        dseconds = dseconds(x),
        dmicroseconds = dmicroseconds(x),
        new_date_1 = date_to_add + dseconds,
        new_date_2 = date_to_add + dseconds - dmicroseconds,
        new_duration = dseconds - dmicroseconds
      ) %>%
      collect(),
    example_d,
    ignore_attr = TRUE
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        r_obj_dseconds = dseconds(1),
        r_obj_dmilliseconds = dmilliseconds(2),
        r_obj_dmicroseconds = dmicroseconds(3),
        r_obj_dnanoseconds = dnanoseconds(4)
      ) %>%
      collect(),
    tibble(),
    ignore_attr = TRUE
  )

  expect_error(
    call_binding("dpicoseconds"),
    "Duration in picoseconds not supported in Arrow"
  )

  expect_error(
    call_binding("lubridate::dpicoseconds"),
    "Duration in picoseconds not supported in Arrow"
  )

  # double -> duration not supported in Arrow.
  # Error is generated in the C++ code
  expect_error(
    test_df %>%
      arrow_table() %>%
      mutate(r_obj_dseconds = dseconds(1.12345)) %>%
      collect()
  )
})

test_that("make_difftime()", {
  test_df <- tibble(
    seconds = c(3, 4, 5, 6),
    minutes = c(1.5, 2.3, 4.5, 6.7),
    hours = c(2, 3, 4, 5),
    days = c(6, 7, 8, 9),
    weeks = c(1, 3, 5, NA),
    number = 10:13
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        duration_from_parts = make_difftime(
          second = seconds,
          minute = minutes,
          hour = hours,
          day = days,
          week = weeks,
          units = "secs"
        ),
        duration_from_num = make_difftime(
          num = number,
          units = "secs"
        ),
        duration_from_r_num = make_difftime(
          num = 154,
          units = "secs"
        ),
        duration_from_r_parts = make_difftime(
          minute = 45,
          day = 2,
          week = 4,
          units = "secs"
        ),
        duration_from_parts2 = lubridate::make_difftime(
          second = seconds,
          minute = minutes,
          hour = hours,
          day = days,
          week = weeks,
          units = "secs"
        )
      ) %>%
      collect(),
    test_df
  )

  # named difftime parts other than `second`, `minute`, `hour`, `day` and `week`
  # are not supported
  expect_error(
    expect_warning(
      test_df %>%
        arrow_table() %>%
        mutate(
          err_difftime = make_difftime(month = 2)
        ) %>%
        collect(),
      paste0(
        "named `difftime` units other than: `second`, `minute`, `hour`,",
        " `day`, and `week` not supported in Arrow."
      )
    )
  )

  # units other than "secs" not supported since they are the only ones in common
  # between R and Arrow
  compare_dplyr_binding(
    .input %>%
      mutate(error_difftime = make_difftime(num = number, units = "mins")) %>%
      collect(),
    test_df,
    warning = TRUE
  )

  # constructing a difftime from both `num` and parts passed through `...` while
  # possible with the lubridate function (resulting in a concatenation of the 2
  # resulting objects), it errors in a dplyr context
  expect_error(
    expect_warning(
      test_df %>%
        arrow_table() %>%
        mutate(
          duration_from_num_and_parts = make_difftime(
            num = number,
            second = seconds,
            minute = minutes,
            hour = hours,
            day = days,
            week = weeks,
            units = "secs"
          )
        ) %>%
        collect(),
      "with both `num` and `...` not supported in Arrow"
    )
  )
})

test_that("`as.Date()` and `as_date()`", {
  test_df <- tibble::tibble(
    posixct_var = as.POSIXct(c("2022-02-25 00:00:01", "1987-11-24 12:34:56", NA), tz = "Pacific/Marquesas"),
    dt_europe = ymd_hms("2010-08-03 00:50:50", "1987-11-24 12:34:56", NA, tz = "Europe/London"),
    dt_utc = ymd_hms("2010-08-03 00:50:50", "1987-11-24 12:34:56", NA),
    date_var = as.Date(c("2022-02-25", "1987-11-24", NA)),
    difference_date = ymd_hms("2010-08-03 00:50:50", "1987-11-24 12:34:56", NA, tz = "Pacific/Marquesas"),
    try_formats_string = c(NA, "2022-01-01", "2022/01/01"),
    character_ymd_hms_var = c("2022-02-25 00:00:01", "1987-11-24 12:34:56", NA),
    character_ydm_hms_var = c("2022/25/02 00:00:01", "1987/24/11 12:34:56", NA),
    character_ymd_var = c("2022-02-25", "1987-11-24", NA),
    character_ydm_var = c("2022/25/02", "1987/24/11", NA),
    integer_var = c(21L, 32L, NA),
    integerish_var = c(21, 32, NA),
    double_var = c(12.34, 56.78, NA)
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        date_dv1 = as.Date(date_var),
        date_dv1_nmspc = base::as.Date(date_var),
        date_pv1 = as.Date(posixct_var),
        date_pv_tz1 = as.Date(posixct_var, tz = "Pacific/Marquesas"),
        date_utc1 = as.Date(dt_utc),
        date_europe1 = as.Date(dt_europe),
        date_char_ymd_hms1 = as.Date(character_ymd_hms_var, format = "%Y-%m-%d %H:%M:%S"),
        date_char_ydm_hms1 = as.Date(character_ydm_hms_var, format = "%Y/%d/%m %H:%M:%S"),
        date_int1 = as.Date(integer_var, origin = "1970-01-01"),
        date_int_origin1 = as.Date(integer_var, origin = "1970-01-03"),
        date_integerish1 = as.Date(integerish_var, origin = "1970-01-01"),
        date_dv2 = as_date(date_var),
        date_dv2_nmspc = lubridate::as_date(date_var),
        date_pv2 = as_date(posixct_var),
        date_pv_tz2 = as_date(posixct_var, tz = "Pacific/Marquesas"),
        date_utc2 = as_date(dt_utc),
        date_europe2 = as_date(dt_europe),
        date_char_ymd2 = as_date(character_ymd_hms_var, format = "%Y-%m-%d %H:%M:%S"),
        date_char_ydm2 = as_date(character_ydm_hms_var, format = "%Y/%d/%m %H:%M:%S"),
        date_int2 = as_date(integer_var, origin = "1970-01-01"),
        date_int_origin2 = as_date(integer_var, origin = "1970-01-03"),
        date_integerish2 = as_date(integerish_var, origin = "1970-01-01")
      ) %>%
      collect(),
    test_df
  )

  # we do not support multiple tryFormats
  # this is not a simple warning, therefore we cannot use compare_dplyr_binding()
  # with `warning = TRUE`
  # arrow_table test
  expect_warning(
    test_df %>%
      arrow_table() %>%
      mutate(
        date_char_ymd = as.Date(
          character_ymd_var,
          tryFormats = c("%Y-%m-%d", "%Y/%m/%d")
        )
      ) %>%
      collect(),
    regexp = "consider using the lubridate specialised parsing functions"
  )

  # record batch test
  expect_warning(
    test_df %>%
      record_batch() %>%
      mutate(
        date_char_ymd = as.Date(
          character_ymd_var,
          tryFormats = c("%Y-%m-%d", "%Y/%m/%d")
        )
      ) %>%
      collect(),
    regexp = "consider using the lubridate specialised parsing functions"
  )

  # strptime does not support a partial format - Arrow returns NA, while
  # lubridate parses correctly
  # TODO: revisit after ARROW-15813
  expect_error(
    expect_equal(
      test_df %>%
        arrow_table() %>%
        mutate(date_char_ymd_hms = as_date(character_ymd_hms_var)) %>%
        collect(),
      test_df %>%
        mutate(date_char_ymd_hms = as_date(character_ymd_hms_var)) %>%
        collect()
    )
  )

  # same as above
  expect_error(
    expect_equal(
      test_df %>%
        arrow_table() %>%
        mutate(date_char_ymd_hms = as.Date(character_ymd_hms_var)) %>%
        collect(),
      test_df %>%
        mutate(date_char_ymd_hms = as.Date(character_ymd_hms_var)) %>%
        collect()
    )
  )

  # we do not support as.Date() with double/ float (error surfaced from C++)
  # TODO: revisit after ARROW-15798
  expect_error(
    test_df %>%
      arrow_table() %>%
      mutate(date_double = as.Date(double_var, origin = "1970-01-01")) %>%
      collect()
  )
  expect_error(
    test_df %>%
      arrow_table() %>%
      mutate(date_double = as_date(double_var, origin = "1970-01-01")) %>%
      collect()
  )

  # difference between as.Date() and as_date():
  # `as.Date()` ignores the `tzone` attribute and uses the value of the `tz` arg
  # to `as.Date()`
  # `as_date()` does the opposite: uses the tzone attribute of the POSIXct object
  # passsed if`tz` is NULL
  compare_dplyr_binding(
    .input %>%
      transmute(
        date_diff_lubridate = as_date(difference_date),
        date_diff_base = as.Date(difference_date)
      ) %>%
      collect(),
    test_df
  )
})

test_that("`as_date()` and `as.Date()` work with R objects", {
  compare_dplyr_binding(
    .input %>%
      mutate(
        date1 = as.Date("2022-05-10"),
        date2 = as.Date(12, origin = "2022-05-01"),
        date3 = as.Date("2022-10-03", tryFormats = "%Y-%m-%d"),
        date4 = as_date("2022-05-10"),
        date5 = as_date(12, origin = "2022-05-01"),
        date6 = as_date("2022-10-03")
      ) %>%
      collect(),
    tibble(
      a = 1
    )
  )
})

test_that("`as_datetime()`", {
  test_df <- tibble(
    date = as.Date(c("2022-03-22", "2021-07-30", NA)),
    char_date = c("2022-03-22", "2021-07-30 14:32:47", NA),
    char_date_non_iso = c("2022-22-03 12:34:56", "2021-30-07 14:32:47", NA),
    int_date = c(10L, 25L, NA),
    integerish_date = c(10, 25, NA),
    double_date = c(10.1, 25.2, NA)
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        ddate = as_datetime(date),
        ddate2 = lubridate::as_datetime(date),
        dchar_date_no_tz = as_datetime(char_date),
        dchar_date_with_tz = as_datetime(char_date, tz = "Pacific/Marquesas"),
        dint_date = as_datetime(int_date, origin = "1970-01-02"),
        dintegerish_date = as_datetime(integerish_date, origin = "1970-01-02"),
        dintegerish_date2 = as_datetime(integerish_date, origin = "1970-01-01")
      ) %>%
      collect(),
    test_df
  )

  # Arrow does not support conversion of double to date
  # the below should error with an error message originating in the C++ code
  expect_error(
    test_df %>%
      arrow_table() %>%
      mutate(
        ddouble_date = as_datetime(double_date)
      ) %>%
      collect(),
    regexp = "Float value 10.1 was truncated converting to int64"
  )
})

test_that("parse_date_time() works with year, month, and date components", {
  # these functions' internals use some string processing which requires the
  # RE2 library (not available on Windows with R 3.6)
  skip_if_not_available("re2")
  compare_dplyr_binding(
    .input %>%
      mutate(
        parsed_date_ymd = parse_date_time(string_ymd, orders = "ymd"),
        parsed_date_ymd2 = lubridate::parse_date_time(string_ymd, orders = "ymd"),
        parsed_date_dmy = parse_date_time(string_dmy, orders = "dmy"),
        parsed_date_mdy = parse_date_time(string_mdy, orders = "mdy")
      ) %>%
      collect(),
    tibble::tibble(
      string_ymd = c(
        "2021-09-1", "2021/09///2", "2021.09.03", "2021,09,4", "2021:09::5",
        "2021 09   6", "21-09-07", "21/09/08", "21.09.9", "21,09,10", "21:09:11",
        "20210912", "210913", NA
      ),
      string_dmy = c(
        "1-09-2021", "2/09//2021", "03.09.2021", "04,09,2021", "5:::09:2021",
        "6  09  2021", "07-09-21", "08/09/21", "9.09.21", "10,09,21", "11:09:21",
        "12092021", "130921", NA
      ),
      string_mdy = c(
        "09-01-2021", "09/2/2021", "09.3.2021", "09,04,2021", "09:05:2021",
        "09 6 2021", "09-7-21", "09/08/21", "09.9.21", "09,10,21", "09:11:21",
        "09122021", "091321", NA
      )
    )
  )

  # TODO(ARROW-16443): locale (affecting "%b% and "%B") does not work on Windows
  skip_on_os("windows")
  compare_dplyr_binding(
    .input %>%
      mutate(
        parsed_date_ymd = parse_date_time(string_ymd, orders = "ymd"),
        parsed_date_dmy = parse_date_time(string_dmy, orders = "dmy"),
        parsed_date_mdy = parse_date_time(string_mdy, orders = "mdy")
      ) %>%
      collect(),
    tibble::tibble(
      string_ymd = c(
        "2021 Sep 12", "2021 September 13", "21 Sep 14", "21 September 15",
        "2021Sep16", NA
      ),
      string_dmy = c(
        "12 Sep 2021", "13 September 2021", "14 Sep 21", "15 September 21",
        "16Sep2021", NA
      ),
      string_mdy = c(
        "Sep 12 2021", "September 13 2021", "Sep 14 21", "September 15 21",
        "Sep1621", NA
      )
    )
  )
})

test_that("parse_date_time() works with a mix of formats and orders", {
  # these functions' internals use some string processing which requires the
  # RE2 library (not available on Windows with R 3.6)
  skip_if_not_available("re2")
  test_df <- tibble(
    string_combi = c("2021-09-1", "2/09//2021", "09.3.2021")
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        date_from_string = parse_date_time(
          string_combi,
          orders = c("ymd", "%d/%m//%Y", "%m.%d.%Y")
        )
      ) %>%
      collect(),
    test_df
  )
})

test_that("year, month, day date/time parsers", {
  test_df <- tibble::tibble(
    ymd_string = c("2022-05-11", "2022/05/12", "22.05-13"),
    ydm_string = c("2022-11-05", "2022/12/05", "22.13-05"),
    mdy_string = c("05-11-2022", "05/12/2022", "05.13-22"),
    myd_string = c("05-2022-11", "05/2022/12", "05.22-14"),
    dmy_string = c("11-05-2022", "12/05/2022", "13.05-22"),
    dym_string = c("11-2022-05", "12/2022/05", "13.22-05")
  )

  # these functions' internals use some string processing which requires the
  # RE2 library (not available on Windows with R 3.6)
  skip_if_not_available("re2")
  compare_dplyr_binding(
    .input %>%
      mutate(
        ymd_date = ymd(ymd_string),
        ydm_date = ydm(ydm_string),
        mdy_date = mdy(mdy_string),
        myd_date = myd(myd_string),
        dmy_date = dmy(dmy_string),
        dym_date = dym(dym_string),
        ymd_date2 = lubridate::ymd(ymd_string),
        ydm_date2 = lubridate::ydm(ydm_string),
        mdy_date2 = lubridate::mdy(mdy_string),
        myd_date2 = lubridate::myd(myd_string),
        dmy_date2 = lubridate::dmy(dmy_string),
        dym_date2 = lubridate::dym(dym_string)
      ) %>%
      collect(),
    test_df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        ymd_date = ymd(ymd_string, tz = "Pacific/Marquesas"),
        ydm_date = ydm(ydm_string, tz = "Pacific/Marquesas"),
        mdy_date = mdy(mdy_string, tz = "Pacific/Marquesas"),
        myd_date = myd(myd_string, tz = "Pacific/Marquesas"),
        dmy_date = dmy(dmy_string, tz = "Pacific/Marquesas"),
        dym_date = dym(dym_string, tz = "Pacific/Marquesas")
      ) %>%
      collect(),
    test_df
  )
})

test_that("ym, my & yq parsers", {
  test_df <- tibble::tibble(
    ym_string = c("2022-05", "2022/02", "22.3", "1979//12", "88.09", NA),
    my_string = c("05-2022", "02/2022", "03.22", "12//1979", "09.88", NA),
    Ym_string = c("2022-05", "2022/02", "2022.03", "1979//12", "1988.09", NA),
    mY_string = c("05-2022", "02/2022", "03.2022", "12//1979", "09.1988", NA),
    yq_string = c("2007.3", "1970.2", "2020.1", "2009.4", "1975.1", NA),
    yq_numeric = c(2007.3, 1970.2, 2020.1, 2009.4, 1975.1, NA),
    yq_space = c("2007 3", "1970 2", "2020 1", "2009 4", "1975 1", NA),
    qy_string = c("3.2007", "2.1970", "1.2020", "4.2009", "1.1975", NA),
    qy_numeric = c(3.2007, 2.1970, 1.2020, 4.2009, 1.1975, NA),
    qy_space = c("3 2007", "2 1970", "1 2020", "4 2009", "1 1975", NA)
  )

  # these functions' internals use some string processing which requires the
  # RE2 library (not available on Windows with R 3.6)
  skip_if_not_available("re2")
  compare_dplyr_binding(
    .input %>%
      mutate(
        ym_date = ym(ym_string),
        ym_date2 = lubridate::ym(ym_string),
        ym_datetime = ym(ym_string, tz = "Pacific/Marquesas"),
        Ym_date = ym(Ym_string),
        Ym_datetime = ym(Ym_string, tz = "Pacific/Marquesas"),
        my_date = my(my_string),
        my_date2 = lubridate::my(my_string),
        my_datetime = my(my_string, tz = "Pacific/Marquesas"),
        mY_date = my(mY_string),
        mY_datetime = my(mY_string, tz = "Pacific/Marquesas"),
        yq_date_from_string = yq(yq_string),
        yq_date_from_string2 = lubridate::yq(yq_string),
        yq_datetime_from_string = yq(yq_string, tz = "Pacific/Marquesas"),
        yq_date_from_numeric = yq(yq_numeric),
        yq_datetime_from_numeric = yq(yq_numeric, tz = "Pacific/Marquesas"),
        yq_date_from_string_with_space = yq(yq_space),
        yq_datetime_from_string_with_space = yq(yq_space, tz = "Pacific/Marquesas"),
        ym_date2 = parse_date_time(ym_string, orders = c("ym", "ymd")),
        my_date2 = parse_date_time(my_string, orders = c("my", "myd")),
        Ym_date2 = parse_date_time(Ym_string, orders = c("Ym", "ymd")),
        mY_date2 = parse_date_time(mY_string, orders = c("mY", "myd")),
        yq_date_from_string2 = parse_date_time(yq_string, orders = "yq"),
        yq_date_from_numeric2 = parse_date_time(yq_numeric, orders = "yq"),
        yq_date_from_string_with_space2 = parse_date_time(yq_space, orders = "yq"),
        # testing with Yq
        yq_date_from_string3 = parse_date_time(yq_string, orders = "Yq"),
        yq_date_from_numeric3 = parse_date_time(yq_numeric, orders = "Yq"),
        yq_date_from_string_with_space3 = parse_date_time(yq_space, orders = "Yq"),
        # testing with qy
        qy_date_from_string = parse_date_time(qy_string, orders = "qy"),
        qy_date_from_numeric = parse_date_time(qy_numeric, orders = "qy"),
        qy_date_from_string_with_space = parse_date_time(qy_space, orders = "qy"),
        # testing with qY
        qy_date_from_string2 = parse_date_time(qy_string, orders = "qY"),
        qy_date_from_numeric2 = parse_date_time(qy_numeric, orders = "qY"),
        qy_date_from_string_with_space2 = parse_date_time(qy_space, orders = "qY")
      ) %>%
      collect(),
    test_df
  )
})

test_that("lubridate's fast_strptime", {
  compare_dplyr_binding(
    .input %>%
      mutate(
        y = fast_strptime(x, format = "%Y-%m-%d %H:%M:%S", lt = FALSE),
        y2 = lubridate::fast_strptime(x, format = "%Y-%m-%d %H:%M:%S", lt = FALSE)
      ) %>%
      collect(),
    tibble(
      x = c("2018-10-07 19:04:05", "2022-05-17 21:23:45", NA)
    )
  )

  # R object
  compare_dplyr_binding(
    .input %>%
      mutate(
        y =
          fast_strptime(
            "68-10-07 19:04:05",
            format = "%y-%m-%d %H:%M:%S",
            lt = FALSE
          )
      ) %>%
      collect(),
    tibble(
      x = c("2018-10-07 19:04:05", NA)
    )
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        date_multi_formats =
          fast_strptime(
            x,
            format = c("%Y-%m-%d %H:%M:%S", "%m-%d-%Y %H:%M:%S"),
            lt = FALSE
          )
      ) %>%
      collect(),
    tibble(
      x = c("2018-10-07 19:04:05", "10-07-1968 19:04:05")
    )
  )

  # these functions' internals use some string processing which requires the
  # RE2 library (not available on Windows with R 3.6)
  skip_if_not_available("re2")

  compare_dplyr_binding(
    .input %>%
      mutate(
        dttm_with_tz = fast_strptime(
          dttm_as_string,
          format = "%Y-%m-%d %H:%M:%S",
          tz = "Pacific/Marquesas",
          lt = FALSE
        )
      ) %>%
      collect(),
    tibble(
      dttm_as_string =
        c("2018-10-07 19:04:05", "1969-10-07 19:04:05", NA)
    )
  )

  # fast_strptime()'s `cutoff_2000` argument is not supported, but its value is
  # implicitly set to 68L both in lubridate and in Arrow
  compare_dplyr_binding(
    .input %>%
      mutate(
        date_short_year =
          fast_strptime(
            x,
            format = "%y-%m-%d %H:%M:%S",
            lt = FALSE
          )
      ) %>%
      collect(),
    tibble(
      x =
        c("68-10-07 19:04:05", "69-10-07 19:04:05", NA)
    )
  )

  # the arrow binding errors for a value different from 68L for `cutoff_2000`
  compare_dplyr_binding(
    .input %>%
      mutate(
        date_short_year =
          fast_strptime(
            x,
            format = "%y-%m-%d %H:%M:%S",
            lt = FALSE,
            cutoff_2000 = 69L
          )
      ) %>%
      collect(),
    tibble(
      x = c("68-10-07 19:04:05", "69-10-07 19:04:05", NA)
    ),
    warning = TRUE
  )

  # compare_dplyr_binding would not work here since lt = TRUE returns a list
  # and it also errors in regular dplyr pipelines
  expect_warning(
    tibble(
      x = c("68-10-07 19:04:05", "69-10-07 19:04:05", NA)
    ) %>%
      arrow_table() %>%
      mutate(
        date_short_year =
          fast_strptime(
            x,
            format = "%y-%m-%d %H:%M:%S",
            lt = TRUE
          )
      ) %>%
      collect()
  )
})

test_that("parse_date_time with hours, minutes and seconds components", {
  test_dates_times <- tibble(
    ymd_hms_string =
      c("67-01-09 12:34:56", "1970-05-22 20:13:59", "870822201359", NA),
    ymd_hm_string =
      c("67-01-09 12:34", "1970-05-22 20:13", "8708222013", NA),
    ymd_h_string =
      c("67-01-09 12", "1970-05-22 20", "87082220", NA),
    dmy_hms_string =
      c("09-01-67 12:34:56", "22-05-1970 20:13:59", "220887201359", NA),
    dmy_hm_string =
      c("09-01-67 12:34", "22-05-1970 20:13", "2208872013", NA),
    dmy_h_string =
      c("09-01-67 12", "22-05-1970 20", "22088720", NA),
    mdy_hms_string =
      c("01-09-67 12:34:56", "05-22-1970 20:13:59", "082287201359", NA),
    mdy_hm_string =
      c("01-09-67 12:34", "05-22-1970 20:13", "0822872013", NA),
    mdy_h_string =
      c("01-09-67 12", "05-22-1970 20", "08228720", NA),
    ydm_hms_string =
      c("67-09-01 12:34:56", "1970-22-05 20:13:59", "872208201359", NA),
    ydm_hm_string =
      c("67-09-01 12:34", "1970-22-05 20:13", "8722082013", NA),
    ydm_h_string =
      c("67-09-01 12", "1970-22-05 20", "87220820", NA)
  )
  # the unseparated strings are versions of "1987-08-22 20:13:59" (with %y)

  # these functions' internals use some string processing which requires the
  # RE2 library (not available on Windows with R 3.6)
  skip_if_not_available("re2")

  compare_dplyr_binding(
    .input %>%
      mutate(
        ymd_hms_dttm = parse_date_time(ymd_hms_string, orders = "ymd_HMS"),
        ymd_hm_dttm  = parse_date_time(ymd_hm_string, orders = "ymd_HM"),
        ymd_h_dttm   = parse_date_time(ymd_h_string, orders = "ymd_H"),
        dmy_hms_dttm = parse_date_time(dmy_hms_string, orders = "dmy_HMS"),
        dmy_hm_dttm  = parse_date_time(dmy_hm_string, orders = "dmy_HM"),
        dmy_h_dttm   = parse_date_time(dmy_h_string, orders = "dmy_H"),
        mdy_hms_dttm = parse_date_time(mdy_hms_string, orders = "mdy_HMS"),
        mdy_hm_dttm  = parse_date_time(mdy_hm_string, orders = "mdy_HM"),
        mdy_h_dttm   = parse_date_time(mdy_h_string, orders = "mdy_H"),
        ydm_hms_dttm = parse_date_time(ydm_hms_string, orders = "ydm_HMS"),
        ydm_hm_dttm  = parse_date_time(ydm_hm_string, orders = "ydmHM"),
        ydm_h_dttm   = parse_date_time(ydm_h_string, orders = "ydmH")
      ) %>%
      collect(),
    test_dates_times
  )

  # parse_date_time with timezone
  pm_tz <- "Pacific/Marquesas"
  compare_dplyr_binding(
    .input %>%
      mutate(
        ymd_hms_dttm = parse_date_time(ymd_hms_string, orders = "ymd_HMS", tz = pm_tz),
        ymd_hm_dttm = parse_date_time(ymd_hm_string, orders = "ymd_HM", tz = pm_tz),
        ymd_h_dttm = parse_date_time(ymd_h_string, orders = "ymd_H", tz = pm_tz),
        dmy_hms_dttm = parse_date_time(dmy_hms_string, orders = "dmy_HMS", tz = pm_tz),
        dmy_hm_dttm = parse_date_time(dmy_hm_string, orders = "dmy_HM", tz = pm_tz),
        dmy_h_dttm = parse_date_time(dmy_h_string, orders = "dmy_H", tz = pm_tz),
        mdy_hms_dttm = parse_date_time(mdy_hms_string, orders = "mdy_HMS", tz = pm_tz),
        mdy_hm_dttm = parse_date_time(mdy_hm_string, orders = "mdy_HM", tz = pm_tz),
        mdy_h_dttm = parse_date_time(mdy_h_string, orders = "mdy_H", tz = pm_tz),
        ydm_hms_dttm = parse_date_time(ydm_hms_string, orders = "ydm_HMS", tz = pm_tz),
        ydm_hm_dttm = parse_date_time(ydm_hm_string, orders = "ydm_HM", tz = pm_tz),
        ydm_h_dttm = parse_date_time(ydm_h_string, orders = "ydm_H", tz = pm_tz)
      ) %>%
      collect(),
    test_dates_times
  )

  # test ymd_ims
  compare_dplyr_binding(
    .input %>%
      mutate(
        ymd_ims_dttm =
          parse_date_time(
            ymd_ims_string,
            orders = "ymd_IMS",
            # lubridate is chatty and will warn 1 format failed to parse
            quiet = TRUE
          )
      ) %>%
      collect(),
    tibble(
      ymd_ims_string =
        c("67-01-09 9:34:56", "1970-05-22 10:13:59", "19870822171359", NA)
    )
  )
})

test_that("parse_date_time with month names and HMS", {
  # TODO(ARROW-16443): locale (affecting "%b% and "%B") does not work on Windows
  skip_on_os("windows")

  # these functions' internals use some string processing which requires the
  # RE2 library (not available on Windows with R 3.6 & the minimal nightly builds)
  skip_if_not_available("re2")

  test_dates_times2 <- tibble(
    ymd_hms_string =
      c("67-Jan-09 12:34:56", "1970-June-22 20:13:59", "87Aug22201359", NA),
    ymd_hm_string =
      c("67-Jan-09 12:34", "1970-June-22 20:13", "87Aug222013", NA),
    ymd_h_string =
      c("67-Jan-09 12", "1970-June-22 20", "87Aug2220", NA),
    dmy_hms_string =
      c("09-Jan-67 12:34:56", "22-June-1970 20:13:59", "22Aug87201359", NA),
    dmy_hm_string =
      c("09-Jan-67 12:34", "22-June-1970 20:13", "22Aug872013", NA),
    dmy_h_string =
      c("09-Jan-67 12", "22-June-1970 20", "22Aug8720", NA),
    mdy_hms_string =
      c("Jan-09-67 12:34:56", "June-22-1970 20:13:59", "Aug2287201359", NA),
    mdy_hm_string =
      c("Jan-09-67 12:34", "June-22-1970 20:13", "Aug22872013", NA),
    mdy_h_string =
      c("Jan-09-67 12", "June-22-1970 20", "Aug228720", NA),
    ydm_hms_string =
      c("67-09-Jan 12:34:56", "1970-22-June 20:13:59", "8722Aug201359", NA),
    ydm_hm_string =
      c("67-09-Jan 12:34", "1970-22-June 20:13", "8722Aug2013", NA),
    ydm_h_string =
      c("67-09-Jan 12", "1970-22-June 20", "8722Aug20", NA)
  )
  # the un-separated strings are versions of "1987-08-22 20:13:59" (with %y)

  compare_dplyr_binding(
    .input %>%
      mutate(
        ymd_hms_dttm = parse_date_time(ymd_hms_string, orders = "ymd_HMS"),
        ymd_hm_dttm  = parse_date_time(ymd_hm_string, orders = "ymdHM"),
        ymd_h_dttm   = parse_date_time(ymd_h_string, orders = "ymd_H"),
        dmy_hms_dttm = parse_date_time(dmy_hms_string, orders = "dmy_HMS"),
        dmy_hm_dttm  = parse_date_time(dmy_hm_string, orders = "dmyHM"),
        dmy_h_dttm   = parse_date_time(dmy_h_string, orders = "dmy_H"),
        mdy_hms_dttm = parse_date_time(mdy_hms_string, orders = "mdy_HMS"),
        mdy_hm_dttm  = parse_date_time(mdy_hm_string, orders = "mdyHM"),
        mdy_h_dttm   = parse_date_time(mdy_h_string, orders = "mdy_H"),
        ydm_hms_dttm = parse_date_time(ydm_hms_string, orders = "ydm_HMS"),
        ydm_hm_dttm  = parse_date_time(ydm_hm_string, orders = "ydmHM"),
        ydm_h_dttm   = parse_date_time(ydm_h_string, orders = "ydm_H")
      ) %>%
      collect(),
    test_dates_times2
  )
})

test_that("parse_date_time with `quiet = FALSE` not supported", {
  # we need expect_warning twice as both the arrow pipeline (because quiet =
  # FALSE is not supported) and the fallback dplyr/lubridate one throw
  # warnings (the lubridate one because quiet is FALSE)
  expect_warning(
    expect_warning(
      tibble(x = c("2022-05-19 13:46:51")) %>%
        arrow_table() %>%
        mutate(
          x_dttm = parse_date_time(x, orders = "dmy_HMS", quiet = FALSE)
        ) %>%
        collect(),
      "`quiet = FALSE` not supported in Arrow"
    ),
    "All formats failed to parse"
  )
})

test_that("parse_date_time with truncated formats", {
  # these functions' internals use some string processing which requires the
  # RE2 library (not available on Windows with R 3.6)
  skip_if_not_available("re2")

  test_truncation_df <- tibble(
    truncated_ymd_string =
      c(
        "2022-05-19 13:46:51",
        "2022-05-18 13:46",
        "2022-05-17 13",
        "2022-05-16"
      )
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        dttm =
          parse_date_time(
            truncated_ymd_string,
            orders = "ymd_HMS",
            truncated = 3
          )
      ) %>%
      collect(),
    test_truncation_df
  )

  # values for truncated greater than nchar(orders) - 3 not supported in Arrow
  compare_dplyr_binding(
    .input %>%
      mutate(
        dttm =
          parse_date_time(
            truncated_ymd_string,
            orders = "ymd_HMS",
            truncated = 5
          )
      ) %>%
      collect(),
    test_truncation_df,
    warning = "a value for `truncated` > 4 not supported in Arrow"
  )
})

test_that("parse_date_time with `exact = TRUE`, and with regular R objects", {
  test_df <- tibble(
    x = c("2022-12-31 12:59:59", "2022-01-01 12:11", "2022-01-01 12", "2022-01-01", NA),
    y = c("11/23/1998 07:00:00", "6/18/1952 0135", "2/25/1974 0523", "9/07/1985 01", NA)
  )

  # these functions' internals use some string processing which requires the
  # RE2 library (not available on Windows with R 3.6)
  skip_if_not_available("re2")
  compare_dplyr_binding(
    .input %>%
      mutate(
        parsed_x =
          parse_date_time(
            x,
            c("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H", "%Y-%m-%d"),
            exact = TRUE
          ),
        parsed_y =
          parse_date_time(
            y,
            c("%m/%d/%Y %I:%M:%S", "%m/%d/%Y %H%M", "%m/%d/%Y %H"),
            exact = TRUE
          )
      ) %>%
      collect(),
    test_df
  )
  compare_dplyr_binding(
    .input %>%
      mutate(
        b = parse_date_time("2022-12-31 12:59:59", orders = "ymd_HMS")
      ) %>%
      collect(),
    tibble(
      a = 1
    )
  )
})

test_that("build_formats() and build_format_from_order()", {
  expect_equal(
    build_formats(c("ym", "myd", "%Y-%d-%m")),
    c(
      # formats from "ym" order
      "%y-%m-%d", "%Y-%m-%d", "%y-%B-%d", "%Y-%B-%d", "%y-%b-%d", "%Y-%b-%d",
      "%y%m%d", "%Y%m%d", "%y%B%d", "%Y%B%d", "%y%b%d", "%Y%b%d",
      # formats from "myd" order
      "%m-%y-%d", "%B-%y-%d", "%b-%y-%d", "%m-%Y-%d", "%B-%Y-%d", "%b-%Y-%d",
      "%m%y%d", "%B%y%d", "%b%y%d", "%m%Y%d", "%B%Y%d", "%b%Y%d",
      # formats from "%Y-%d-%m" format
      "%y-%d-%m", "%Y-%d-%m", "%y-%d-%B", "%Y-%d-%B", "%y-%d-%b", "%Y-%d-%b",
      "%y%d%m", "%Y%d%m", "%y%d%B", "%Y%d%B", "%y%d%b", "%Y%d%b"
    )
  )

  expect_equal(
    build_formats("ymd_HMS"),
    c(
      "%y-%m-%d-%H-%M-%S", "%Y-%m-%d-%H-%M-%S", "%y-%B-%d-%H-%M-%S",
      "%Y-%B-%d-%H-%M-%S", "%y-%b-%d-%H-%M-%S", "%Y-%b-%d-%H-%M-%S",
      "%y%m%d%H%M%S", "%Y%m%d%H%M%S", "%y%B%d%H%M%S", "%Y%B%d%H%M%S",
      "%y%b%d%H%M%S", "%Y%b%d%H%M%S"
    )
  )

  # when order is one of "yq", "qy", "ym" or"my" the data is augmented to "ymd"
  # or "ydm" and the formats are built accordingly
  ymd_formats <- c(
    "%y-%m-%d", "%Y-%m-%d", "%y-%B-%d", "%Y-%B-%d", "%y-%b-%d", "%Y-%b-%d",
    "%y%m%d", "%Y%m%d", "%y%B%d", "%Y%B%d", "%y%b%d", "%Y%b%d"
  )
  expect_equal(
    build_formats("yq"),
    ymd_formats
  )

  expect_equal(
    build_formats("ym"),
    ymd_formats
  )

  expect_equal(
    build_formats("qy"),
    ymd_formats
  )

  # build formats will output unique formats
  expect_equal(
    build_formats(c("yq", "ym", "qy")),
    ymd_formats
  )

  expect_equal(
    build_formats("my"),
    c(
      "%m-%y-%d", "%B-%y-%d", "%b-%y-%d", "%m-%Y-%d", "%B-%Y-%d", "%b-%Y-%d",
      "%m%y%d", "%B%y%d", "%b%y%d", "%m%Y%d", "%B%Y%d", "%b%Y%d"
    )
  )

  # ab not supported yet
  expect_error(
    build_formats("abd"),
    '"abd" `orders` not supported in Arrow'
  )

  expect_error(
    build_formats("vup"),
    '"vup" `orders` not supported in Arrow'
  )

  expect_equal(
    build_format_from_order("ymd"),
    c(
      "%y-%m-%d", "%Y-%m-%d", "%y-%B-%d", "%Y-%B-%d", "%y-%b-%d", "%Y-%b-%d",
      "%y%m%d", "%Y%m%d", "%y%B%d", "%Y%B%d", "%y%b%d", "%Y%b%d"
    )
  )

  expect_equal(
    build_format_from_order("ymdHMS"),
    c(
      "%y-%m-%d-%H-%M-%S", "%Y-%m-%d-%H-%M-%S", "%y-%B-%d-%H-%M-%S",
      "%Y-%B-%d-%H-%M-%S", "%y-%b-%d-%H-%M-%S", "%Y-%b-%d-%H-%M-%S",
      "%y%m%d%H%M%S", "%Y%m%d%H%M%S", "%y%B%d%H%M%S", "%Y%B%d%H%M%S",
      "%y%b%d%H%M%S", "%Y%b%d%H%M%S"
    )
  )
})
