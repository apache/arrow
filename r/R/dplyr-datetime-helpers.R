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
  if (tolower(Sys.info()[["sysname"]]) == "windows" & locale != "C") {
    # MingW C++ std::locale only supports "C" and "POSIX"
    stop(paste0(
      "On Windows, time locales other than 'C' are not supported in Arrow. ",
      "Consider setting `Sys.setlocale('LC_TIME', 'C')`"
    ))
  }
  locale
}

.helpers_function_map <- list(
  "dminutes" = list(60, "s"),
  "dhours" = list(3600, "s"),
  "ddays" = list(86400, "s"),
  "dweeks" = list(604800, "s"),
  "dmonths" = list(2629800, "s"),
  "dyears" = list(31557600, "s"),
  "dseconds" = list(1, "s"),
  "dmilliseconds" = list(1, "ms"),
  "dmicroseconds" = list(1, "us"),
  "dnanoseconds" = list(1, "ns")
)
make_duration <- function(x, unit) {
  x <- build_expr("cast", x, options = cast_options(to_type = int64()))
  x$cast(duration(unit))
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
  if (is.null(format) && length(tryFormats) > 1) {
    abort("`as.Date()` with multiple `tryFormats` is not supported in Arrow")
  }

  if (call_binding("is.Date", x)) {
    return(x)

    # cast from character
  } else if (call_binding("is.character", x)) {
    x <- binding_as_date_character(x, format, tryFormats)

    # cast from numeric
  } else if (call_binding("is.numeric", x)) {
    x <- binding_as_date_numeric(x, origin)
  }

  build_expr("cast", x, options = cast_options(to_type = date32()))
}

binding_as_date_character <- function(x,
                                      format = NULL,
                                      tryFormats = "%Y-%m-%d") {
  format <- format %||% tryFormats[[1]]
  # unit = 0L is the identifier for seconds in valid_time32_units
  build_expr("strptime", x, options = list(format = format, unit = 0L))
}

binding_as_date_numeric <- function(x, origin = "1970-01-01") {

  # Arrow does not support direct casting from double to date32(), but for
  # integer-like values we can go via int32()
  # https://issues.apache.org/jira/browse/ARROW-15798
  # TODO revisit if arrow decides to support double -> date casting
  if (!call_binding("is.integer", x)) {
    x <- build_expr("cast", x, options = cast_options(to_type = int32()))
  }

  if (origin != "1970-01-01") {
    delta_in_sec <- call_binding("difftime", origin, "1970-01-01")
    # TODO: revisit once either of these issues is addressed:
    #   https://issues.apache.org/jira/browse/ARROW-16253 (helper function for
    #   casting from double to duration) or
    #   https://issues.apache.org/jira/browse/ARROW-15862 (casting from int32
    #   -> duration or double -> duration)
    delta_in_sec <- build_expr("cast", delta_in_sec, options = cast_options(to_type = int64()))
    delta_in_days <- (delta_in_sec / 86400L)$cast(int32())
    x <- build_expr("+", x, delta_in_days)
  }

  x
}

build_formats <- function(orders) {
  # only keep the letters and the underscore as separator -> allow the users to
  # pass strptime-like formats (with "%"). Processing is needed (instead of passing
  # formats as-is) due to the processing of the character vector in parse_date_time()
  orders <- gsub("[^A-Za-z_]", "", orders)
  orders <- gsub("Y", "y", orders)

  supported_orders <- c("ymd", "ydm", "mdy", "myd", "dmy", "dym")
  unsupported_passed_orders <- setdiff(orders, supported_orders)
  supported_passed_orders <- intersect(orders, supported_orders)

  # error only if there isn't at least one valid order we can try
  if (length(supported_passed_orders) == 0) {
    arrow_not_supported(
      paste0(
        oxford_paste(
          unsupported_passed_orders
        ),
        " `orders`"
      )
    )
  }

  formats_list <- map(orders, build_format_from_order)
  purrr::flatten_chr(formats_list)
}

build_format_from_order <- function(order) {
  year_chars <- c("%y", "%Y")
  month_chars <- c("%m", "%B", "%b")
  day_chars <- "%d"

  outcome <- switch(
    order,
    "ymd" = expand.grid(year_chars, month_chars, day_chars),
    "ydm" = expand.grid(year_chars, day_chars, month_chars),
    "mdy" = expand.grid(month_chars, day_chars, year_chars),
    "myd" = expand.grid(month_chars, year_chars, day_chars),
    "dmy" = expand.grid(day_chars, month_chars, year_chars),
    "dym" = expand.grid(day_chars, year_chars, month_chars)
  )
  outcome$format <- paste(outcome$Var1, outcome$Var2, outcome$Var3, sep = "-")
  outcome$format
}
