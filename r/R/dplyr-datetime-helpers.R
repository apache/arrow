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

check_time_locale <- function(locale = Sys.getlocale("LC_TIME")) {
  if (tolower(Sys.info()[["sysname"]]) == "windows" && locale != "C") {
    # MingW C++ std::locale only supports "C" and "POSIX"
    stop(paste0(
      "On Windows, time locales other than 'C' are not supported in Arrow. ",
      "Consider setting `Sys.setlocale('LC_TIME', 'C')`"
    ))
  }
  locale
}

make_duration <- function(x, unit) {
  # TODO(ARROW-15862): remove first cast to int64
  cast(x, int64())$cast(duration(unit))
}

binding_format_datetime <- function(x, format = "", tz = "", usetz = FALSE) {
  if (usetz) {
    format <- paste(format, "%Z")
  }

  if (call_binding("is.POSIXct", x)) {
    # Make sure the timezone is reflected
    if (tz == "" && x$type()$timezone() != "") {
      tz <- x$type()$timezone()
    } else if (tz == "") {
      tz <- Sys.timezone()
    }
    x <- cast(x, timestamp(x$type()$unit(), tz))
  }
  opts <- list(format = format, locale = Sys.getlocale("LC_TIME"))
  Expression$create("strftime", x, options = opts)
}

# this is a helper function used for creating a difftime / duration objects from
# several of the accepted pieces (second, minute, hour, day, week)
duration_from_chunks <- function(chunks) {
  accepted_chunks <- c("second", "minute", "hour", "day", "week")
  matched_chunks <- accepted_chunks[pmatch(names(chunks), accepted_chunks, duplicates.ok = TRUE)]

  if (any(is.na(matched_chunks))) {
    abort(
      paste0(
        "named `difftime` units other than: ",
        oxford_paste(accepted_chunks, quote_symbol = "`"),
        " not supported in Arrow. \nInvalid `difftime` parts: ",
        oxford_paste(names(chunks[is.na(matched_chunks)]), quote_symbol = "`")
      )
    )
  }

  matched_chunks <- matched_chunks[!is.na(matched_chunks)]

  chunks <- chunks[matched_chunks]
  chunk_duration <- c(
    "second" = 1L,
    "minute" = 60L,
    "hour" = 3600L,
    "day" = 86400L,
    "week" = 604800L
  )

  # transform the duration of each chunk in seconds and add everything together
  duration <- 0
  for (chunk in names(chunks)) {
    duration <- duration + chunks[[chunk]] * chunk_duration[[chunk]]
  }
  duration
}


binding_as_date <- function(x,
                            format = NULL,
                            tryFormats = "%Y-%m-%d",
                            origin = "1970-01-01") {
  if (call_binding("is.Date", x)) {
    return(x)

    # cast from character
  } else if (call_binding("is.character", x)) {
    x <- binding_as_date_character(x, format, tryFormats)

    # cast from numeric
  } else if (call_binding("is.numeric", x)) {
    x <- binding_as_date_numeric(x, origin)
  }

  cast(x, date32())
}

binding_as_date_character <- function(x,
                                      format = NULL,
                                      tryFormats = "%Y-%m-%d") {
  format <- format %||% tryFormats[[1]]
  # unit = 0L is the identifier for seconds in valid_time32_units
  Expression$create("strptime", x, options = list(format = format, unit = 0L))
}

binding_as_date_numeric <- function(x, origin = "1970-01-01") {

  # Arrow does not support direct casting from double to date32(), but for
  # integer-like values we can go via int32()
  # TODO: revisit after ARROW-15798
  if (!call_binding("is.integer", x)) {
    x <- cast(x, int32())
  }

  if (origin != "1970-01-01") {
    delta_in_sec <- call_binding("difftime", origin, "1970-01-01")
    # TODO: revisit after ARROW-15862
    # (casting from int32 -> duration or double -> duration)
    delta_in_days <- cast(
      cast(delta_in_sec, int64()) / 86400L,
      int32()
    )
    x <- call_binding("+", x, delta_in_days)
  }

  x
}

#' Build formats from multiple orders
#'
#' This function is a vectorised version of `build_format_from_order()`. In
#' addition to `build_format_from_order()`, it also checks if the supplied
#' orders are currently supported.
#'
#' @inheritParams process_data_for_parsing
#'
#' @return a vector of unique formats
#'
#' @noRd
build_formats <- function(orders) {
  # only keep the letters and the underscore as separator -> allow the users to
  # pass strptime-like formats (with "%"). We process the data -> we need to
  # process the `orders` (even if supplied in the desired format)
  # Processing is needed (instead of passing
  # formats as-is) due to the processing of the character vector in parse_date_time()

  orders <- gsub("[^A-Za-z]", "", orders)
  orders <- gsub("Y", "y", orders)

  valid_formats <- "[a|A|b|B|d|H|I|j|m|Om|M|Op|p|q|OS|S|U|w|W|y|Y|r|R|T|z]"
  invalid_orders <- nchar(gsub(valid_formats, "", orders)) > 0

  if (any(invalid_orders)) {
    arrow_not_supported(
      paste0(
        oxford_paste(
          orders[invalid_orders]
        ),
        " `orders`"
      )
    )
  }

  # we separate "ym', "my", and "yq" from the rest of the `orders` vector and
  # transform them. `ym` and `yq` -> `ymd` & `my` -> `myd`
  # this is needed for 2 reasons:
  # 1. strptime does not parse "2022-05" -> we add "-01", thus changing the format,
  # 2. for equivalence to lubridate, which parses `ym` to the first day of the month
  short_orders <- c("ym", "my", "yOm", "Omy")
  quarter_orders <- c("yq", "qy")

  if (any(orders %in% short_orders)) {
    orders1 <- setdiff(orders, short_orders)
    orders2 <- intersect(orders, short_orders)
    orders2 <- paste0(orders2, "d")
    orders <- unique(c(orders2, orders1))
  }
  if (any(orders %in% quarter_orders)) {
    orders <- c(setdiff(orders, quarter_orders), "ymd")
  }
  orders <- unique(orders)

  formats_list <- map(orders, build_format_from_order)
  formats <- purrr::flatten_chr(formats_list)
  unique(formats)
}

#' Build formats from a single order
#'
#' @param order a single string date-time format, such as `"ymd"` or `"ymd_hms"`
#'
#' @return a vector of all possible formats derived from the input
#' order
#'
#' @noRd
build_format_from_order <- function(order) {
  month_formats <- c("%m", "%B", "%b")
  week_formats <- c("%a", "%A")
  year_formats <- c("%y", "%Y")
  char_list <- list(
    "%y" = year_formats,
    "%Y" = year_formats,
    "%m" = month_formats,
    "%Om" = month_formats,
    "%b" = month_formats,
    "%B" = month_formats,
    "%a" = week_formats,
    "%A" = week_formats,
    "%d" = "%d",
    "%H" = "%H",
    "%j" = "%j",
    "%OS" = "%OS",
    "%I" = "%I",
    "%S" = "%S",
    "%q" = "%q",
    "%M" = "%M",
    "%U" = "%U",
    "%w" = "%w",
    "%W" = "%W",
    "%p" = "%p",
    "%Op" = "%Op",
    "%z" = "%z",
    "%r" = c("%H", "%I-%p"),
    "%R" = c("%H-%M", "%I-%M-%p"),
    "%T" = c("%I-%M-%S-%p", "%H-%M-%S", "%H-%M-%OS")
  )

  split_order <- regmatches(order, gregexpr("(O{0,1}[a-zA-Z])", order))[[1]]
  split_order <- paste0("%", split_order)
  outcome <- expand.grid(char_list[split_order])

  # we combine formats with and without the "-" separator, we will later
  # coalesce through all of them (benchmarking indicated this is a more
  # computationally efficient approach rather than figuring out if a string has
  # separators or not and applying the relevant order afterwards)
  formats_with_sep <- do.call(paste, c(outcome, sep = "-"))
  formats_without_sep <- gsub("-", "", formats_with_sep)
  c(formats_with_sep, formats_without_sep)
}

#' Process data in preparation for parsing
#'
#' `process_data_for_parsing()` takes a data column and a vector of `orders` and
#' prepares several versions of the input data:
#'   * `processed_x` is a version of `x` where all separators were replaced with
#'  `"-"` and multiple separators were collapsed into a single one. This element
#'  is only set to an empty list when the `orders` argument indicate we're only
#'  interested in parsing the augmented version of `x`.
#'  * each of the other 3 elements augment `x` in some way
#'    * `augmented_x_ym` - builds the `ym` and `my` formats by adding `"01"`
#'    (to indicate the first day of the month)
#'    * `augmented_x_yq` - transforms the `yq` format to `ymd`, by deriving the
#'    first month of the quarter and adding `"01"` to indicate the first day
#'    * `augmented_x_qy` - transforms the `qy` format to `ymd` in a similar
#'    manner to `"yq"`
#'
#' @param x an Expression corresponding to a character or numeric vector of
#' dates to be parsed.
#' @param orders a character vector of date-time formats.
#'
#' @return a list made up of 4 lists, each a different version of x:
#'  * `processed_x`
#'  * `augmented_x_ym`
#'  * `augmented_x_yq`
#'  * `augmented_x_qy`
#' @noRd
process_data_for_parsing <- function(x, orders) {
  processed_x <- x$cast(string())

  # make all separators (non-letters and non-numbers) into "-"
  processed_x <- call_binding("gsub", "[^A-Za-z0-9]", "-", processed_x)
  # collapse multiple separators into a single one
  processed_x <- call_binding("gsub", "-{2,}", "-", processed_x)

  # we need to transform `x` when orders are `ym`, `my`, and `yq`
  # for `ym` and `my` orders we add a day ("01")
  # TODO: revisit after ARROW-16627
  augmented_x_ym <- NULL
  if (any(orders %in% c("ym", "my", "Ym", "mY"))) {
    # add day as "-01" if there is a "-" separator and as "01" if not
    augmented_x_ym <- call_binding(
      "if_else",
      call_binding("grepl", "-", processed_x),
      call_binding("paste0", processed_x, "-01"),
      call_binding("paste0", processed_x, "01")
    )
  }

  # for `yq` we need to transform the quarter into the start month (lubridate
  # behaviour) and then add 01 to parse to the first day of the quarter
  augmented_x_yq <- NULL
  if (any(orders %in% c("yq", "Yq"))) {
    # extract everything that comes after the `-` separator, i.e. the quarter
    # (e.g. 4 from 2022-4)
    quarter_x <- call_binding("gsub", "^.*?-", "", processed_x)
    # we should probably error if quarter is not in 1:4
    # extract everything that comes before the `-`, i.e. the year (e.g. 2002
    # in 2002-4)
    year_x <- call_binding("gsub", "-.*$", "", processed_x)
    quarter_x <- quarter_x$cast(int32())
    month_x <- (quarter_x - 1) * 3 + 1
    augmented_x_yq <- call_binding("paste0", year_x, "-", month_x, "-01")
  }

  # same as for `yq`, we need to derive the month from the quarter and add a
  # "01" to give us the first day of the month
  augmented_x_qy <- NULL
  if (any(orders %in% c("qy", "qY"))) {
    quarter_x <- call_binding("gsub", "-.*$", "", processed_x)
    quarter_x <- quarter_x$cast(int32())
    year_x <- call_binding("gsub", "^.*?-", "", processed_x)
    # year might be missing the final 0s when extracted from a float, hence the
    # need to pad
    year_x <- call_binding("str_pad", year_x, width = 4, side = "right", pad = "0")
    month_x <- (quarter_x - 1) * 3 + 1
    augmented_x_qy <- call_binding("paste0", year_x, "-", month_x, "-01")
  }

  list(
    "augmented_x_ym" = augmented_x_ym,
    "augmented_x_yq" = augmented_x_yq,
    "augmented_x_qy" = augmented_x_qy,
    "processed_x" = processed_x
  )
}


#' Attempt parsing
#'
#' This function does several things:
#'   * builds all possible `formats` from the supplied `orders`
#'   * processes the data with `process_data_for_parsing()`
#'   * build a list of the possible `strptime` Expressions for the data & formats
#'   combinations
#'
#' @inheritParams process_data_for_parsing
#'
#' @return a list of `strptime` Expressions we can use with `coalesce`
#' @noRd
attempt_parsing <- function(x, orders) {
  # translate orders into possible formats
  formats <- build_formats(orders)

  # depending on the orders argument we need to do some processing to the input
  # data. `process_data_for_parsing()` uses the passed `orders` and not the
  # derived `formats`
  processed_data <- process_data_for_parsing(x, orders)

  # build a list of expressions for parsing each processed_data element and
  # format combination
  parse_attempt_exprs_list <- map(processed_data, build_strptime_exprs, formats)

  # if all orders are in c("ym", "my", "yq", "qy") only attempt to parse the
  # augmented version(s) of x
  if (all(orders %in% c("ym", "Ym", "my", "mY", "yq", "Yq", "qy", "qY"))) {
    parse_attempt_exprs_list$processed_x <- list()
  }

  # we need the output to be a list of expressions (currently it is a list of
  # lists of expressions due to the shape of the processed data. we have one list
  # of expressions for each element of/ list in processed_data) -> we need to
  # remove a level of hierarchy from the list
  purrr::flatten(parse_attempt_exprs_list)
}

#' Build `strptime` expressions
#'
#' This function takes several `formats`, iterates over them and builds a
#' `strptime` Expression for each of them. Given these Expressions are evaluated
#' row-wise we can leverage this behaviour and introduce a condition. If `x` has
#' a separator, use the `format` as is, if it doesn't have a separator, remove
#' the `"-"` separator from the `format`.
#'
#' @param x an Expression corresponding to a character or numeric vector of
#' dates to be parsed.
#' @param formats a character vector of formats as returned by
#' `build_format_from_order`
#'
#' @return a list of Expressions
#' @noRd
build_strptime_exprs <- function(x, formats) {
  # returning an empty list helps when iterating
  if (is.null(x)) {
    return(list())
  }

  map(
    formats,
    ~ Expression$create(
      "strptime",
      x,
      options = list(format = .x, unit = 0L, error_is_null = TRUE)
    )
  )
}

# This function parses the "unit" argument to round_date, floor_date, and
# ceiling_date. The input x is a single string like "second", "3 seconds",
# "10 microseconds" or "2 secs" used to specify the size of the unit to
# which the temporal data should be rounded. The matching rules implemented
# are designed to mirror lubridate exactly: it extracts the numeric multiple
# from the start of the string (presumed to be 1 if no number is present)
# and selects the unit by looking at the first 3 characters only. This choice
# ensures that "secs", "second", "microsecs" etc are all valid, but it is
# very permissive and would interpret "mickeys" as microseconds. This
# permissive implementation mirrors the corresponding implementation in
# lubridate. The return value is a list with integer-valued components
# "multiple" and  "unit"
parse_period_unit <- function(x) {
  # the regexp matches against fractional units, but per lubridate
  # supports integer multiples of a known unit only
  match_info <- regexpr(
    pattern = " *(?<multiple>[0-9.,]+)? *(?<unit>[^ \t\n]+)",
    text = x[[1]],
    perl = TRUE
  )

  capture_start <- attr(match_info, "capture.start")
  capture_length <- attr(match_info, "capture.length")
  capture_end <- capture_start + capture_length - 1L

  str_unit <- substr(x, capture_start[[2]], capture_end[[2]])
  str_multiple <- substr(x, capture_start[[1]], capture_end[[1]])

  known_units <- c(
    "nanosecond", "microsecond", "millisecond", "second",
    "minute", "hour", "day", "week", "month", "quarter", "year"
  )

  # match the period unit
  str_unit_start <- substr(str_unit, 1, 3)
  unit <- as.integer(pmatch(str_unit_start, known_units)) - 1L

  if (any(is.na(unit))) {
    abort(
      sprintf(
        "Invalid period name: '%s'",
        str_unit,
        ". Known units are",
        oxford_paste(known_units, "and")
      )
    )
  }

  # empty string in multiple interpreted as 1
  if (capture_length[[1]] == 0) {
    multiple <- 1L

    # otherwise parse the multiple
  } else {
    multiple <- as.numeric(str_multiple)

    # special cases: interpret fractions of 1 second as integer
    # multiples of nanoseconds, microseconds, or milliseconds
    # to mirror lubridate syntax
    if (unit == 3L) {
      if (multiple < 10^-6) {
        unit <- 0L
        multiple <- 10^9 * multiple
      }
      if (multiple < 10^-3) {
        unit <- 1L
        multiple <- 10^6 * multiple
      }
      if (multiple < 1) {
        unit <- 2L
        multiple <- 10^3 * multiple
      }
    }

    multiple <- as.integer(multiple)
  }

  # more special cases: lubridate imposes sensible maximum
  # values on the number of seconds, minutes and hours
  if (unit == 3L && multiple > 60) {
    abort("Rounding with second > 60 is not supported")
  }
  if (unit == 4L && multiple > 60) {
    abort("Rounding with minute > 60 is not supported")
  }
  if (unit == 5L && multiple > 24) {
    abort("Rounding with hour > 24 is not supported")
  }

  list(unit = unit, multiple = multiple)
}

# This function handles round/ceil/floor when unit is week. The fn argument
# specifies which of the temporal rounding functions (round_date, etc) is to
# be applied, x is the data argument to the rounding function, week_start is
# an integer indicating which day of the week is the start date. The C++
# library natively handles Sunday and Monday so in those cases we pass the
# week_starts_monday option through. Other week_start values are handled here
shift_temporal_to_week <- function(fn, x, week_start, options) {
  if (week_start == 7) { # Sunday
    options$week_starts_monday <- FALSE
    return(Expression$create(fn, x, options = options))
  }

  if (week_start == 1) { # Monday
    options$week_starts_monday <- TRUE
    return(Expression$create(fn, x, options = options))
  }

  # other cases use offset-from-Monday: to ensure type-stable output there
  # are two separate helpers, one to handle date32 input and the other to
  # handle timestamps
  options$week_starts_monday <- TRUE
  offset <- as.integer(week_start) - 1L

  is_date32 <- inherits(x, "Date") ||
    (inherits(x, "Expression") && x$type_id() == Type$DATE32)

  if (is_date32) {
    shifted_date <- shift_date32_to_week(fn, x, offset, options = options)
  } else {
    shifted_date <- shift_timestamp_to_week(fn, x, offset, options = options)
  }

  shifted_date
}

# timestamp input should remain timestamp
shift_timestamp_to_week <- function(fn, x, offset, options) {
  # Convert offset to duration(s) and make Expression once
  offset_seconds <- Expression$scalar(
    cast(
      Scalar$create(offset * 86400L, int64()),
      duration(unit = "s")
    )
  )

  # Subtract offset, apply round/floor/ceil, then add the offset back
  shifted <- x - offset_seconds
  shift_offset <- Expression$create(fn, shifted, options = options)
  shift_offset + offset_seconds
}

# to avoid date32 types being cast to timestamp during the temporal
# arithmetic, the offset logic needs to use the count in days and
# use integer arithmetic: this feels inelegant, but it ensures that
# temporal rounding functions remain type stable
shift_date32_to_week <- function(fn, x, offset, options) {
  # offset is R integer, make it an Expression once
  offset <- Expression$scalar(offset)

  # Subtract offset as int32, then cast back to date32
  x_offset <- cast(
    cast(x, int32()) - offset,
    date32()
  )

  # apply round/floor/ceil
  shift_offset <- Expression$create(fn, x_offset, options = options)

  # undo offset (as integer) and return
  cast(
    cast(shift_offset, int32()) + offset,
    date32()
  )
}
