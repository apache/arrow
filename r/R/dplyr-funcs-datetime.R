

register_datetime_translations <- function() {

  nse_funcs$strptime <- function(x, format = "%Y-%m-%d %H:%M:%S", tz = NULL, unit = "ms") {
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
  }

  nse_funcs$strftime <- function(x, format = "", tz = "", usetz = FALSE) {
    if (usetz) {
      format <- paste(format, "%Z")
    }
    if (tz == "") {
      tz <- Sys.timezone()
    }
    # Arrow's strftime prints in timezone of the timestamp. To match R's strftime behavior we first
    # cast the timestamp to desired timezone. This is a metadata only change.
    if (nse_funcs$is.POSIXct(x)) {
      ts <- Expression$create("cast", x, options = list(to_type = timestamp(x$type()$unit(), tz)))
    } else {
      ts <- x
    }
    Expression$create("strftime", ts, options = list(format = format, locale = Sys.getlocale("LC_TIME")))
  }

  nse_funcs$format_ISO8601 <- function(x, usetz = FALSE, precision = NULL, ...) {
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
  }

  nse_funcs$second <- function(x) {
    Expression$create("add", Expression$create("second", x), Expression$create("subsecond", x))
  }

  nse_funcs$wday <- function(x,
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
      return(Expression$create("strftime", x, options = list(format = format, locale = locale)))
    }

    Expression$create("day_of_week", x, options = list(count_from_zero = FALSE, week_start = week_start))
  }

  nse_funcs$month <- function(x, label = FALSE, abbr = TRUE, locale = Sys.getlocale("LC_TIME")) {
    if (label) {
      if (abbr) {
        format <- "%b"
      } else {
        format <- "%B"
      }
      return(Expression$create("strftime", x, options = list(format = format, locale = locale)))
    }

    Expression$create("month", x)
  }

  nse_funcs$is.Date <- function(x) {
    inherits(x, "Date") ||
      (inherits(x, "Expression") && x$type_id() %in% Type[c("DATE32", "DATE64")])
  }

  nse_funcs$is.instant <- nse_funcs$is.timepoint <- function(x) {
    inherits(x, c("POSIXt", "POSIXct", "POSIXlt", "Date")) ||
      (inherits(x, "Expression") && x$type_id() %in% Type[c("TIMESTAMP", "DATE32", "DATE64")])
  }

  nse_funcs$is.POSIXct <- function(x) {
    inherits(x, "POSIXct") ||
      (inherits(x, "Expression") && x$type_id() %in% Type[c("TIMESTAMP")])
  }
}
