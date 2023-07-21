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

# Split up into several register functions by category to reduce cyclomatic
# complexity (linter)
register_bindings_datetime <- function() {
  register_bindings_datetime_utility()
  register_bindings_datetime_components()
  register_bindings_datetime_conversion()
  register_bindings_datetime_timezone()
  register_bindings_duration()
  register_bindings_duration_constructor()
  register_bindings_duration_helpers()
  register_bindings_datetime_parsers()
  register_bindings_datetime_rounding()
}

register_bindings_datetime_utility <- function() {
  register_binding(
    "base::strptime",
    function(x,
             format = "%Y-%m-%d %H:%M:%S",
             tz = "",
             unit = "ms") {
      # Arrow uses unit for time parsing, strptime() does not.
      # Arrow has no default option for strptime (format, unit),
      # we suggest following format = "%Y-%m-%d %H:%M:%S", unit = MILLI/1L/"ms",
      # (ARROW-12809)

      unit <- make_valid_time_unit(
        unit,
        c(valid_time64_units, valid_time32_units)
      )

      output <- Expression$create(
        "strptime",
        x,
        options =
          list(
            format = format,
            unit = unit,
            error_is_null = TRUE
          )
      )

      if (tz == "") {
        tz <- Sys.timezone()
      }

      # If a timestamp does not contain timezone information (i.e. it is
      # "timezone-naive") we can attach timezone information (i.e. convert it into
      # a "timezone-aware" timestamp) with `assume_timezone`
      # if we want to cast to a different timezone, we can only do it for
      # timezone-aware timestamps, not for timezone-naive ones.
      # strptime in Acero will return a timezone-aware timestamp if %z is
      # part of the format string.
      if (!is.null(tz) && !grepl("%z", format, fixed = TRUE)) {
        output <- Expression$create(
          "assume_timezone",
          output,
          options =
            list(
              timezone = tz
            )
        )
      }
      output
    },
    notes = c(
      "accepts a `unit` argument not present in the `base` function.",
      'Valid values are "s", "ms" (default), "us", "ns".'
    )
  )

  register_binding("base::strftime", function(x,
                                              format = "",
                                              tz = "",
                                              usetz = FALSE) {
    if (usetz) {
      format <- paste(format, "%Z")
    }
    if (tz == "") {
      tz <- Sys.timezone()
    }
    # Arrow's strftime prints in timezone of the timestamp. To match R's strftime behavior we first
    # cast the timestamp to desired timezone. This is a metadata only change.
    if (call_binding("is.POSIXct", x)) {
      ts <- Expression$create("cast", x, options = list(to_type = timestamp(x$type()$unit(), tz)))
    } else {
      ts <- x
    }
    Expression$create("strftime", ts, options = list(format = format, locale = check_time_locale()))
  })

  register_binding("lubridate::format_ISO8601", function(x, usetz = FALSE, precision = NULL, ...) {
    ISO8601_precision_map <-
      list(
        y = "%Y",
        ym = "%Y-%m",
        ymd = "%Y-%m-%d",
        ymdh = "%Y-%m-%dT%H",
        ymdhm = "%Y-%m-%dT%H:%M",
        ymdhms = "%Y-%m-%dT%H:%M:%S"
      )

    if (is.null(precision)) {
      precision <- "ymdhms"
    }
    if (!precision %in% names(ISO8601_precision_map)) {
      abort(
        paste(
          "`precision` must be one of the following values:",
          paste(names(ISO8601_precision_map), collapse = ", "),
          "\nValue supplied was: ",
          precision
        )
      )
    }
    format <- ISO8601_precision_map[[precision]]
    if (usetz) {
      format <- paste0(format, "%z")
    }
    Expression$create("strftime", x, options = list(format = format, locale = "C"))
  })

  register_binding("lubridate::is.Date", function(x) {
    inherits(x, "Date") ||
      (inherits(x, "Expression") && x$type_id() %in% Type[c("DATE32", "DATE64")])
  })

  is_instant_binding <- function(x) {
    inherits(x, c("POSIXt", "POSIXct", "POSIXlt", "Date")) ||
      (inherits(x, "Expression") && x$type_id() %in% Type[c("TIMESTAMP", "DATE32", "DATE64")])
  }
  register_binding("lubridate::is.instant", is_instant_binding)
  register_binding("lubridate::is.timepoint", is_instant_binding)

  register_binding("lubridate::is.POSIXct", function(x) {
    inherits(x, "POSIXct") ||
      (inherits(x, "Expression") && x$type_id() %in% Type[c("TIMESTAMP")])
  })

  register_binding("lubridate::date", function(x) {
    cast(x, date32())
  })
}

register_bindings_datetime_components <- function() {
  register_binding("lubridate::second", function(x) {
    Expression$create("add", Expression$create("second", x), Expression$create("subsecond", x))
  })

  register_binding("lubridate::wday", function(x,
                                               label = FALSE,
                                               abbr = TRUE,
                                               week_start = getOption("lubridate.week.start", 7),
                                               locale = Sys.getlocale("LC_TIME")) {
    if (label) {
      if (abbr) {
        format <- "%a"
      } else {
        format <- "%A"
      }
      return(Expression$create("strftime", x, options = list(format = format, locale = check_time_locale(locale))))
    }

    Expression$create("day_of_week", x, options = list(count_from_zero = FALSE, week_start = week_start))
  })

  register_binding("lubridate::week", function(x) {
    (call_binding("yday", x) - 1) %/% 7 + 1
  })

  register_binding("lubridate::month", function(x,
                                                label = FALSE,
                                                abbr = TRUE,
                                                locale = Sys.getlocale("LC_TIME")) {
    if (call_binding("is.integer", x)) {
      x <- call_binding(
        "if_else",
        call_binding("between", x, 1, 12),
        x,
        NA_integer_
      )
      if (!label) {
        # if we don't need a label we can return the integer itself (already
        # constrained to 1:12)
        return(x)
      }
      # make the integer into a date32() - which interprets integers as
      # days from epoch (we multiply by 28 to be able to later extract the
      # month with label) - NB this builds a false date (to be used by strftime)
      # since we only know and care about the month
      x <- cast(x * 28L, date32())
    }

    if (label) {
      if (abbr) {
        format <- "%b"
      } else {
        format <- "%B"
      }
      return(Expression$create("strftime", x, options = list(format = format, locale = check_time_locale(locale))))
    }

    Expression$create("month", x)
  })

  register_binding("lubridate::qday", function(x) {
    # We calculate day of quarter by flooring timestamp to beginning of quarter and
    # calculating days between beginning of quarter and timestamp/date in question.
    # Since we use one one-based numbering we add one.
    floored_x <- Expression$create("floor_temporal", x, options = list(unit = 9L))
    Expression$create("days_between", floored_x, x) + Expression$scalar(1L)
  })

  register_binding("lubridate::am", function(x) {
    hour <- Expression$create("hour", x)
    hour < 12
  })
  register_binding("lubridate::pm", function(x) {
    !call_binding("am", x)
  })
  register_binding("lubridate::tz", function(x) {
    if (!call_binding("is.POSIXct", x)) {
      arrow_not_supported(
        paste0(
          "timezone extraction for objects of class `",
          infer_type(x)$ToString(),
          "`"
        )
      )
    }

    x$type()$timezone()
  })
  register_binding("lubridate::semester", function(x, with_year = FALSE) {
    month <- call_binding("month", x)
    semester <- Expression$create("if_else", month <= 6, 1L, 2L)
    if (with_year) {
      year <- call_binding("year", x)
      return(year + semester / 10)
    } else {
      return(semester)
    }
  })
}

register_bindings_datetime_conversion <- function() {
  register_binding(
    "lubridate::make_datetime",
    function(year = 1970L,
             month = 1L,
             day = 1L,
             hour = 0L,
             min = 0L,
             sec = 0,
             tz = "UTC") {
      # ParseTimestampStrptime currently ignores the timezone information (ARROW-12820).
      # Stop if tz other than 'UTC' is provided.
      if (tz != "UTC") {
        arrow_not_supported("Time zone other than 'UTC'")
      }

      x <- call_binding("str_c", year, month, day, hour, min, sec, sep = "-")
      Expression$create("strptime", x, options = list(format = "%Y-%m-%d-%H-%M-%S", unit = 0L))
    },
    notes = "only supports UTC (default) timezone"
  )

  register_binding("lubridate::make_date", function(year = 1970L,
                                                    month = 1L,
                                                    day = 1L) {
    x <- call_binding("make_datetime", year, month, day)
    cast(x, date32())
  })

  register_binding("base::ISOdatetime", function(year,
                                                 month,
                                                 day,
                                                 hour,
                                                 min,
                                                 sec,
                                                 tz = "UTC") {
    # NAs for seconds aren't propagated (but treated as 0) in the base version
    sec <- call_binding(
      "if_else",
      call_binding("is.na", sec),
      0,
      sec
    )

    call_binding("make_datetime", year, month, day, hour, min, sec, tz)
  })

  register_binding("base::ISOdate", function(year,
                                             month,
                                             day,
                                             hour = 12,
                                             min = 0,
                                             sec = 0,
                                             tz = "UTC") {
    call_binding("make_datetime", year, month, day, hour, min, sec, tz)
  })

  register_binding(
    "base::as.Date",
    function(x,
             format = NULL,
             tryFormats = "%Y-%m-%d",
             origin = "1970-01-01",
             tz = "UTC") {
      if (is.null(format) && length(tryFormats) > 1) {
        abort(
          paste(
            "`as.Date()` with multiple `tryFormats` is not supported in Arrow.",
            "Consider using the lubridate specialised parsing functions `ymd()`, `ymd()`, etc."
          )
        )
      }

      # base::as.Date() and lubridate::as_date() differ in the way they use the
      # `tz` argument. Both cast to the desired timezone, if present. The
      # difference appears when the `tz` argument is not set: `as.Date()` uses the
      # default value ("UTC"), while `as_date()` keeps the original attribute
      # => we only cast when we want the behaviour of the base version or when
      # `tz` is set (i.e. not NULL)
      if (call_binding("is.POSIXct", x)) {
        unit <- if (inherits(x, "Expression")) x$type()$unit() else "s"
        x <- cast(x, timestamp(unit = unit, timezone = tz))
      }

      binding_as_date(
        x = x,
        format = format,
        tryFormats = tryFormats,
        origin = origin
      )
    },
    notes = c(
      "Multiple `tryFormats` not supported in Arrow.",
      "Consider using the lubridate specialised parsing functions `ymd()`, `ymd()`, etc."
    )
  )

  register_binding("lubridate::as_date", function(x,
                                                  format = NULL,
                                                  origin = "1970-01-01",
                                                  tz = NULL) {
    # base::as.Date() and lubridate::as_date() differ in the way they use the
    # `tz` argument. Both cast to the desired timezone, if present. The
    # difference appears when the `tz` argument is not set: `as.Date()` uses the
    # default value ("UTC"), while `as_date()` keeps the original attribute
    # => we only cast when we want the behaviour of the base version or when
    # `tz` is set (i.e. not NULL)
    if (call_binding("is.POSIXct", x) && !is.null(tz)) {
      unit <- if (inherits(x, "Expression")) x$type()$unit() else "s"
      x <- cast(x, timestamp(unit = unit, timezone = tz))
    }
    binding_as_date(
      x = x,
      format = format,
      origin = origin
    )
  })

  register_binding("lubridate::as_datetime", function(x,
                                                      origin = "1970-01-01",
                                                      tz = "UTC",
                                                      format = NULL,
                                                      unit = "ns") {
    # Arrow uses unit for time parsing, as_datetime() does not.
    unit <- make_valid_time_unit(
      unit,
      c(valid_time64_units, valid_time32_units)
    )

    if (call_binding("is.integer", x)) {
      x <- cast(x, int64())
    }

    if (call_binding("is.numeric", x)) {
      multiple <- Expression$create("power_checked", 1000L, unit)
      delta <- call_binding("difftime", origin, "1970-01-01")
      delta <- cast(delta, int64())
      delta <- Expression$create("multiply_checked", delta, multiple)
      x <- Expression$create("multiply_checked", x, multiple)
      x <- cast(x, int64())
      x <- Expression$create("add_checked", x, delta)
    }

    if (call_binding("is.character", x) && !is.null(format)) {
      x <- Expression$create(
        "strptime",
        x,
        options = list(format = format, unit = unit, error_is_null = TRUE)
      )
    }
    output <- cast(x, timestamp(unit = unit))
    Expression$create("assume_timezone", output, options = list(timezone = tz))
  })

  register_binding("lubridate::decimal_date", function(date) {
    y <- Expression$create("year", date)
    start <- call_binding("make_datetime", year = y, tz = "UTC")
    sofar <- call_binding("difftime", date, start, units = "secs")
    total <- Expression$create(
      "if_else",
      Expression$create("is_leap_year", date),
      Expression$scalar(31622400L), # number of seconds in a leap year (366 days)
      Expression$scalar(31536000L) # number of seconds in a regular year (365 days)
    )
    y + cast(sofar, int64()) / total
  })

  register_binding("lubridate::date_decimal", function(decimal, tz = "UTC") {
    y <- Expression$create("floor", decimal)

    start <- call_binding("make_datetime", year = y, tz = tz)
    seconds <- Expression$create(
      "if_else",
      Expression$create("is_leap_year", start),
      Expression$scalar(31622400L), # number of seconds in a leap year (366 days)
      Expression$scalar(31536000L) # number of seconds in a regular year (365 days)
    )

    fraction <- decimal - y
    delta <- Expression$create("floor", seconds * fraction)
    delta <- make_duration(delta, "s")
    start + delta
  })
}

register_bindings_datetime_timezone <- function() {
  register_binding(
    "lubridate::force_tz",
    function(time, tzone = "", roll_dst = c("error", "post")) {
      if (length(roll_dst) == 1L) {
        roll_dst <- c(roll_dst, roll_dst)
      } else if (length(roll_dst) != 2L) {
        arrow_not_supported("`roll_dst` must be 1 or 2 items long; other lengths")
      }

      nonexistent <- switch(
        roll_dst[1],
        "error" = 0L,
        "boundary" = 2L,
        arrow_not_supported("`roll_dst` value must be 'error' or 'boundary' for non-existent times; other values")
      )

      ambiguous <- switch(
        roll_dst[2],
        "error" = 0L,
        "pre" = 1L,
        "post" = 2L,
        arrow_not_supported("`roll_dst` value must be 'error', 'pre', or 'post' for non-existent times")
      )

      if (identical(tzone, "")) {
        tzone <- Sys.timezone()
      }

      if (!inherits(time, "Expression")) {
        time <- Expression$scalar(time)
      }

      # Non-UTC timezones don't work here and getting them to do so was too
      # hard to do in the initial PR because there is no way in Arrow to
      # "unapply" a UTC offset (i.e., the reverse of assume_timezone).
      if (!time$type()$timezone() %in% c("", "UTC")) {
        arrow_not_supported("`time` with a non-UTC timezone")
      }

      # Remove timezone if needed
      current_unit <- time$type()$unit()
      time <- cast(time, timestamp(current_unit, ""))

      # Add timezone
      Expression$create(
        "assume_timezone",
        time,
        options = list(
          timezone = tzone,
          nonexistent = nonexistent,
          ambiguous = ambiguous
        )
      )
    },
    notes = c(
      "Timezone conversion from non-UTC timezone not supported;",
      "`roll_dst` values of 'error' and 'boundary' are supported for nonexistent times,",
      "`roll_dst` values of 'error', 'pre', and 'post' are supported for ambiguous times."
    )
  )

  register_binding("lubridate::with_tz", function(time, tzone = "") {
    if (tzone == "") {
      tzone <- Sys.timezone()
    }
    cast(time, timestamp(unit = time$type()$unit(), timezone = tzone))
  })
}

register_bindings_duration <- function() {
  register_binding(
    "base::difftime",
    function(time1, time2, tz, units = "secs") {
      if (units != "secs") {
        arrow_not_supported("`difftime()` with units other than `secs`")
      }

      if (!missing(tz)) {
        warn("`tz` argument is not supported in Arrow, so it will be ignored")
      }

      # cast to timestamp if time1 and time2 are not dates or timestamp expressions
      # (the subtraction of which would output a `duration`)
      if (!call_binding("is.instant", time1)) {
        time1 <- cast(time1, timestamp())
      }

      if (!call_binding("is.instant", time2)) {
        time2 <- cast(time2, timestamp())
      }

      # if time1 or time2 are timestamps they cannot be expressed in "s" /seconds
      # otherwise they cannot be added subtracted with durations
      # TODO delete the casting to "us" once
      # https://issues.apache.org/jira/browse/ARROW-16060 is solved
      if (inherits(time1, "Expression") &&
        time1$type_id() %in% Type[c("TIMESTAMP")] && time1$type()$unit() != 2L) {
        time1 <- cast(time1, timestamp("us"))
      }

      if (inherits(time2, "Expression") &&
        time2$type_id() %in% Type[c("TIMESTAMP")] && time2$type()$unit() != 2L) {
        time2 <- cast(time2, timestamp("us"))
      }

      # we need to go build the subtract expression instead of `time1 - time2` to
      # prevent complaints when we try to subtract an R object from an Expression
      cast(call_binding("-", time1, time2), duration("s"))
    },
    notes = c(
      'only supports `units = "secs"` (the default);',
      "`tz` argument not supported"
    )
  )

  register_binding(
    "base::as.difftime",
    function(x, format = "%X", units = "secs") {
      # windows doesn't seem to like "%X"
      if (format == "%X" & tolower(Sys.info()[["sysname"]]) == "windows") {
        format <- "%H:%M:%S"
      }

      if (units != "secs") {
        arrow_not_supported("`as.difftime()` with units other than 'secs'")
      }

      if (call_binding("is.character", x)) {
        x <- Expression$create("strptime", x, options = list(format = format, unit = 0L))
        # we do a final cast to duration ("s") at the end
        x <- make_duration(cast(x, time64("us")), unit = "us")
      }

      # numeric -> duration not supported in Arrow yet so we use int64() as an
      # intermediate step
      # TODO: revisit after ARROW-15862

      if (call_binding("is.numeric", x)) {
        # coerce x to be int64(). it should work for integer-like doubles and fail
        # for pure doubles
        # if we abort for all doubles, we risk erroring in cases in which
        # coercion to int64() would work
        x <- cast(x, int64())
      }

      cast(x, duration(unit = "s"))
    },
    notes = 'only supports `units = "secs"` (the default)'
  )
}

register_bindings_duration_constructor <- function() {
  register_binding(
    "lubridate::make_difftime",
    function(num = NULL, units = "secs", ...) {
      if (units != "secs") {
        arrow_not_supported("`make_difftime()` with units other than 'secs'")
      }

      chunks <- list(...)

      # lubridate concatenates durations passed via the `num` argument with those
      # passed via `...` resulting in a vector of length 2 - which is virtually
      # unusable in a dplyr pipeline. Arrow errors in this situation
      if (!is.null(num) && length(chunks) > 0) {
        arrow_not_supported("`make_difftime()` with both `num` and `...`")
      }

      if (!is.null(num)) {
        # build duration from num if present
        duration <- num
      } else {
        # build duration from chunks when nothing is passed via ...
        duration <- duration_from_chunks(chunks)
      }

      make_duration(duration, "s")
    },
    notes = c(
      'only supports `units = "secs"` (the default);',
      "providing both `num` and `...` is not supported"
    )
  )
}

register_bindings_duration_helpers <- function() {
  duration_factory <- function(value, unit) {
    force(value)
    force(unit)
    function(x = 1) make_duration(x * value, unit)
  }

  register_binding("lubridate::dminutes", duration_factory(60, "s"))
  register_binding("lubridate::dhours", duration_factory(3600, "s"))
  register_binding("lubridate::ddays", duration_factory(86400, "s"))
  register_binding("lubridate::dweeks", duration_factory(604800, "s"))
  register_binding("lubridate::dmonths", duration_factory(2629800, "s"))
  register_binding("lubridate::dyears", duration_factory(31557600, "s"))
  register_binding("lubridate::dseconds", duration_factory(1, "s"))
  register_binding("lubridate::dmilliseconds", duration_factory(1, "ms"))
  register_binding("lubridate::dmicroseconds", duration_factory(1, "us"))
  register_binding("lubridate::dnanoseconds", duration_factory(1, "ns"))
  register_binding(
    "lubridate::dpicoseconds",
    function(x = 1) {
      abort("Duration in picoseconds not supported in Arrow.")
    },
    notes = "not supported"
  )
}

register_bindings_datetime_parsers <- function() {
  register_binding(
    "lubridate::parse_date_time",
    function(x,
             orders,
             tz = "UTC",
             truncated = 0,
             quiet = TRUE,
             exact = FALSE) {
      if (!quiet) {
        arrow_not_supported("`quiet = FALSE`")
      }

      if (truncated > 0) {
        if (truncated > (nchar(orders) - 3)) {
          arrow_not_supported(paste0("a value for `truncated` > ", nchar(orders) - 3))
        }
        # build several orders for truncated formats
        orders <- map_chr(0:truncated, ~ substr(orders, start = 1, stop = nchar(orders) - .x))
      }

      if (!inherits(x, "Expression")) {
        x <- Expression$scalar(x)
      }

      if (exact == TRUE) {
        # no data processing takes place & we don't derive formats
        parse_attempts <- build_strptime_exprs(x, orders)
      } else {
        parse_attempts <- attempt_parsing(x, orders = orders)
      }

      coalesce_output <- Expression$create("coalesce", args = parse_attempts)

      # we need this binding to be able to handle a NULL `tz`, which, in turn,
      # will be used by bindings such as `ymd()` to return a date or timestamp,
      # based on whether tz is NULL or not
      if (!is.null(tz)) {
        Expression$create("assume_timezone", coalesce_output, options = list(timezone = tz))
      } else {
        coalesce_output
      }
    },
    notes = c(
      "`quiet = FALSE` is not supported",
      "Available formats are H, I, j, M, S, U, w, W, y, Y, R, T.",
      "On Linux and OS X additionally a, A, b, B, Om, p, r are available."
    )
  )

  parser_vec <- c(
    "ymd", "ydm", "mdy", "myd", "dmy", "dym", "ym", "my", "yq",
    "ymd_HMS", "ymd_HM", "ymd_H", "dmy_HMS", "dmy_HM", "dmy_H",
    "mdy_HMS", "mdy_HM", "mdy_H", "ydm_HMS", "ydm_HM", "ydm_H"
  )

  parser_map_factory <- function(order) {
    force(order)
    function(x, quiet = TRUE, tz = NULL, locale = NULL, truncated = 0) {
      if (!is.null(locale)) {
        arrow_not_supported("`locale`")
      }
      # Parsers returning datetimes return UTC by default and never return dates.
      if (is.null(tz) && nchar(order) > 3) {
        tz <- "UTC"
      }
      parse_x <- call_binding("parse_date_time", x, order, tz, truncated, quiet)
      if (is.null(tz)) {
        # we cast so we can mimic the behaviour of the `tz` argument in lubridate
        # "If NULL (default), a Date object is returned. Otherwise a POSIXct with
        # time zone attribute set to tz."
        parse_x <- cast(parse_x, date32())
      }
      parse_x
    }
  }

  for (order in parser_vec) {
    register_binding(
      paste0("lubridate::", tolower(order)),
      parser_map_factory(order),
      notes = "`locale` argument not supported"
    )
  }

  register_binding(
    "lubridate::fast_strptime",
    function(x, format, tz = "UTC", lt = FALSE, cutoff_2000 = 68L) {
      # `lt` controls the output `lt = TRUE` returns a POSIXlt (which doesn't play
      # well with mutate, for example)
      if (lt) {
        arrow_not_supported("`lt = TRUE` argument")
      }

      # TODO revisit after https://issues.apache.org/jira/browse/ARROW-16596
      if (cutoff_2000 != 68L) {
        arrow_not_supported("`cutoff_2000` != 68L argument")
      }

      parse_attempt_expressions <- list()

      parse_attempt_expressions <- map(
        format,
        ~ Expression$create(
          "strptime",
          x,
          options = list(
            format = .x,
            unit = 0L,
            error_is_null = TRUE
          )
        )
      )

      coalesce_output <- Expression$create("coalesce", args = parse_attempt_expressions)

      Expression$create("assume_timezone", coalesce_output, options = list(timezone = tz))
    },
    notes = "non-default values of `lt` and `cutoff_2000` not supported"
  )
}

register_bindings_datetime_rounding <- function() {
  register_binding(
    "lubridate::round_date",
    function(x,
             unit = "second",
             week_start = getOption("lubridate.week.start", 7)) {
      opts <- parse_period_unit(unit)
      if (opts$unit == 7L) { # weeks (unit = 7L) need to accommodate week_start
        return(shift_temporal_to_week("round_temporal", x, week_start, options = opts))
      }

      Expression$create("round_temporal", x, options = opts)
    }
  )

  register_binding(
    "lubridate::floor_date",
    function(x,
             unit = "second",
             week_start = getOption("lubridate.week.start", 7)) {
      opts <- parse_period_unit(unit)
      if (opts$unit == 7L) { # weeks (unit = 7L) need to accommodate week_start
        return(shift_temporal_to_week("floor_temporal", x, week_start, options = opts))
      }

      Expression$create("floor_temporal", x, options = opts)
    }
  )

  register_binding(
    "lubridate::ceiling_date",
    function(x,
             unit = "second",
             change_on_boundary = NULL,
             week_start = getOption("lubridate.week.start", 7)) {
      opts <- parse_period_unit(unit)
      if (is.null(change_on_boundary)) {
        change_on_boundary <- ifelse(call_binding("is.Date", x), TRUE, FALSE)
      }
      opts$ceil_is_strictly_greater <- change_on_boundary

      if (opts$unit == 7L) { # weeks (unit = 7L) need to accommodate week_start
        return(shift_temporal_to_week("ceil_temporal", x, week_start, options = opts))
      }

      Expression$create("ceil_temporal", x, options = opts)
    }
  )
}
