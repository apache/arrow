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

register_bindings_datetime <- function() {
  register_binding("strptime", function(x, format = "%Y-%m-%d %H:%M:%S", tz = NULL,
                                            unit = "ms") {
    # Arrow uses unit for time parsing, strptime() does not.
    # Arrow has no default option for strptime (format, unit),
    # we suggest following format = "%Y-%m-%d %H:%M:%S", unit = MILLI/1L/"ms",
    # (ARROW-12809)

    # ParseTimestampStrptime currently ignores the timezone information (ARROW-12820).
    # Stop if tz is provided.
    if (is.character(tz)) {
      arrow_not_supported("Time zone argument")
    }

    unit <- make_valid_time_unit(unit, c(valid_time64_units, valid_time32_units))

    Expression$create("strptime", x, options = list(format = format, unit = unit))
  })

  register_binding("strftime", function(x, format = "", tz = "", usetz = FALSE) {
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
    Expression$create("strftime", ts, options = list(format = format, locale = Sys.getlocale("LC_TIME")))
  })

  register_binding("format_ISO8601", function(x, usetz = FALSE, precision = NULL, ...) {
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

  register_binding("second", function(x) {
    Expression$create("add", Expression$create("second", x), Expression$create("subsecond", x))
  })

  register_binding("wday", function(x, label = FALSE, abbr = TRUE,
                                        week_start = getOption("lubridate.week.start", 7),
                                        locale = Sys.getlocale("LC_TIME")) {
    if (label) {
      if (abbr) {
        format <- "%a"
      } else {
        format <- "%A"
      }
      return(Expression$create("strftime", x, options = list(format = format, locale = locale)))
    }

    Expression$create("day_of_week", x, options = list(count_from_zero = FALSE, week_start = week_start))
  })

  register_binding("week", function(x) {
    (call_binding("yday", x) - 1) %/% 7 + 1
  })

  register_binding("month", function(x,
                                     label = FALSE,
                                     abbr = TRUE,
                                     locale = Sys.getlocale("LC_TIME")) {

    if (call_binding("is.integer", x)) {
      x <- call_binding("if_else",
                        call_binding("between", x, 1, 12),
                        x,
                        NA_integer_)
      if (!label) {
        # if we don't need a label we can return the integer itself (already
        # constrained to 1:12)
        return(x)
      }
        # make the integer into a date32() - which interprets integers as
        # days from epoch (we multiply by 28 to be able to later extract the
        # month with label) - NB this builds a false date (to be used by strftime)
        # since we only know and care about the month
        x <- build_expr("cast", x * 28L, options = cast_options(to_type = date32()))
    }

    if (label) {
      if (abbr) {
        format <- "%b"
      } else {
        format <- "%B"
      }
      return(build_expr("strftime", x, options = list(format = format, locale = locale)))
    }

    build_expr("month", x)
  })

  register_binding("is.Date", function(x) {
    inherits(x, "Date") ||
      (inherits(x, "Expression") && x$type_id() %in% Type[c("DATE32", "DATE64")])
  })

  is_instant_binding <- function(x) {
    inherits(x, c("POSIXt", "POSIXct", "POSIXlt", "Date")) ||
      (inherits(x, "Expression") && x$type_id() %in% Type[c("TIMESTAMP", "DATE32", "DATE64")])
  }
  register_binding("is.instant", is_instant_binding)
  register_binding("is.timepoint", is_instant_binding)

  register_binding("is.POSIXct", function(x) {
    inherits(x, "POSIXct") ||
      (inherits(x, "Expression") && x$type_id() %in% Type[c("TIMESTAMP")])
  })

  register_binding("leap_year", function(date) {
    Expression$create("is_leap_year", date)
  })

  register_binding("am", function(x) {
    hour <- Expression$create("hour", x)
    hour < 12
  })
  register_binding("pm", function(x) {
    !call_binding("am", x)
  })
  register_binding("tz", function(x) {
    if (!call_binding("is.POSIXct", x)) {
      abort(paste0("timezone extraction for objects of class `", type(x)$ToString(), "` not supported in Arrow"))
    }

    x$type()$timezone()
  })
  register_binding("semester", function(x, with_year = FALSE) {
    month <- call_binding("month", x)
    semester <- call_binding("if_else", month <= 6, 1L, 2L)
    if (with_year) {
      year <- call_binding("year", x)
      return(year + semester / 10)
    } else {
      return(semester)
    }
  })
  register_binding("date", function(x) {
    build_expr("cast", x, options = list(to_type = date32()))
  })
}

binding_format_datetime <- function(x, format = "", tz = "", usetz = FALSE) {
  if (usetz) {
    format <- paste(format, "%Z")
  }

  if (call_binding("is.POSIXct", x)) {
    # the casting part might not be required once
    # https://issues.apache.org/jira/browse/ARROW-14442 is solved
    # TODO revisit the steps below once the PR for that issue is merged
    if (tz == "" && x$type()$timezone() != "") {
      tz <- x$type()$timezone()
    } else if (tz == "") {
      tz <- Sys.timezone()
    }
    x <- build_expr("cast", x, options = cast_options(to_type = timestamp(x$type()$unit(), tz)))
  }

  build_expr("strftime", x, options = list(format = format, locale = Sys.getlocale("LC_TIME")))
}
