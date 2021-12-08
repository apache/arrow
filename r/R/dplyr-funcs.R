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


#' @include expression.R
NULL

# This environment contains a cache of nse_funcs as a list()
.cache <- new.env(parent = emptyenv())

# Called in .onLoad()
create_translation_cache <- function() {
  arrow_funcs <- list()

  # Register all available Arrow Compute functions, namespaced as arrow_fun.
  if (arrow_available()) {
    all_arrow_funs <- list_compute_functions()
    arrow_funcs <- set_names(
      lapply(all_arrow_funs, function(fun) {
        force(fun)
        function(...) build_expr(fun, ...)
      }),
      paste0("arrow_", all_arrow_funs)
    )
  }

  # Register translations into nse_funcs and agg_funcs
  register_array_function_map_translations()
  register_aggregate_translations()
  register_conditional_translations()
  register_datetime_translations()
  register_math_translations()
  register_string_translations()
  register_type_translations()

  # We only create the cache for nse_funcs and not agg_funcs
  .cache$functions <- c(as.list(nse_funcs), arrow_funcs)
}

# nse_funcs is a list of functions that operated on (and return) Expressions
# These will be the basis for a data_mask inside dplyr methods
# and will be added to .cache at package load time
nse_funcs <- new.env(parent = emptyenv())

# agg_funcs is a list of functions with a different signature than nse_funcs;
# described below
agg_funcs <- new.env(parent = emptyenv())

translation_registry <- function() {
  nse_funcs
}

translation_registry_agg <- function() {
  agg_funcs
}

register_translation <- function(fun_name, fun, registry = translation_registry()) {
  name <- gsub("^.*?::", "", fun_name)
  namespace <- gsub("::.*$", "", fun_name)

  previous_fun <- if (name %in% names(fun)) registry[[name]] else NULL

  if (is.null(fun)) {
    rm(list = name, envir = registry)
  } else {
    registry[[name]] <- fun
  }

  invisible(previous_fun)
}

register_translation_agg <- function(fun_name, fun, registry = translation_registry_agg()) {
  register_translation(fun_name, fun, registry = registry)
}

# Start with mappings from R function name spellings
register_array_function_map_translations <- function() {
  # use a function to generate the binding so that `operator` persists
  # beyond execution time (another option would be to use quasiquotation
  # and unquote `operator` directly into the function expression)
  array_function_map_factory <- function(operator) {
    force(operator)
    function(...) build_expr(operator, ...)
  }

  for (name in names(.array_function_map)) {
    register_translation(name, array_function_map_factory(name))
  }
}

# Now add functions to that list where the mapping from R to Arrow isn't 1:1
# Each of these functions should have the same signature as the R function
# they're replacing.
#
# When to use `build_expr()` vs. `Expression$create()`?
#
# Use `build_expr()` if you need to
# (1) map R function names to Arrow C++ functions
# (2) wrap R inputs (vectors) as Array/Scalar
#
# `Expression$create()` is lower level. Most of the functions below use it
# because they manage the preparation of the user-provided inputs
# and don't need to wrap scalars

register_type_translations <- function() {

  nse_funcs$cast <- function(x, target_type, safe = TRUE, ...) {
    opts <- cast_options(safe, ...)
    opts$to_type <- as_type(target_type)
    Expression$create("cast", x, options = opts)
  }

  nse_funcs$is.na <- function(x) {
    build_expr("is_null", x, options = list(nan_is_null = TRUE))
  }

  nse_funcs$is.nan <- function(x) {
    if (is.double(x) || (inherits(x, "Expression") &&
      x$type_id() %in% TYPES_WITH_NAN)) {
      # TODO: if an option is added to the is_nan kernel to treat NA as NaN,
      # use that to simplify the code here (ARROW-13366)
      build_expr("is_nan", x) & build_expr("is_valid", x)
    } else {
      Expression$scalar(FALSE)
    }
  }

  nse_funcs$is <- function(object, class2) {
    if (is.string(class2)) {
      switch(class2,
        # for R data types, pass off to is.*() functions
        character = nse_funcs$is.character(object),
        numeric = nse_funcs$is.numeric(object),
        integer = nse_funcs$is.integer(object),
        integer64 = nse_funcs$is.integer64(object),
        logical = nse_funcs$is.logical(object),
        factor = nse_funcs$is.factor(object),
        list = nse_funcs$is.list(object),
        # for Arrow data types, compare class2 with object$type()$ToString(),
        # but first strip off any parameters to only compare the top-level data
        # type,  and canonicalize class2
        sub("^([^([<]+).*$", "\\1", object$type()$ToString()) ==
          canonical_type_str(class2)
      )
    } else if (inherits(class2, "DataType")) {
      object$type() == as_type(class2)
    } else {
      stop("Second argument to is() is not a string or DataType", call. = FALSE)
    }
  }

  nse_funcs$dictionary_encode <- function(x,
                                          null_encoding_behavior = c("mask", "encode")) {
    behavior <- toupper(match.arg(null_encoding_behavior))
    null_encoding_behavior <- NullEncodingBehavior[[behavior]]
    Expression$create(
      "dictionary_encode",
      x,
      options = list(null_encoding_behavior = null_encoding_behavior)
    )
  }

  nse_funcs$between <- function(x, left, right) {
    x >= left & x <= right
  }

  nse_funcs$is.finite <- function(x) {
    is_fin <- Expression$create("is_finite", x)
    # for compatibility with base::is.finite(), return FALSE for NA_real_
    is_fin & !nse_funcs$is.na(is_fin)
  }

  nse_funcs$is.infinite <- function(x) {
    is_inf <- Expression$create("is_inf", x)
    # for compatibility with base::is.infinite(), return FALSE for NA_real_
    is_inf & !nse_funcs$is.na(is_inf)
  }

  # as.* type casting functions
  # as.factor() is mapped in expression.R
  nse_funcs$as.character <- function(x) {
    Expression$create("cast", x, options = cast_options(to_type = string()))
  }
  nse_funcs$as.double <- function(x) {
    Expression$create("cast", x, options = cast_options(to_type = float64()))
  }
  nse_funcs$as.integer <- function(x) {
    Expression$create(
      "cast",
      x,
      options = cast_options(
        to_type = int32(),
        allow_float_truncate = TRUE,
        allow_decimal_truncate = TRUE
      )
    )
  }
  nse_funcs$as.integer64 <- function(x) {
    Expression$create(
      "cast",
      x,
      options = cast_options(
        to_type = int64(),
        allow_float_truncate = TRUE,
        allow_decimal_truncate = TRUE
      )
    )
  }
  nse_funcs$as.logical <- function(x) {
    Expression$create("cast", x, options = cast_options(to_type = boolean()))
  }
  nse_funcs$as.numeric <- function(x) {
    Expression$create("cast", x, options = cast_options(to_type = float64()))
  }

  # is.* type functions
  nse_funcs$is.character <- function(x) {
    is.character(x) || (inherits(x, "Expression") &&
      x$type_id() %in% Type[c("STRING", "LARGE_STRING")])
  }
  nse_funcs$is.numeric <- function(x) {
    is.numeric(x) || (inherits(x, "Expression") && x$type_id() %in% Type[c(
      "UINT8", "INT8", "UINT16", "INT16", "UINT32", "INT32",
      "UINT64", "INT64", "HALF_FLOAT", "FLOAT", "DOUBLE",
      "DECIMAL128", "DECIMAL256"
    )])
  }
  nse_funcs$is.double <- function(x) {
    is.double(x) || (inherits(x, "Expression") && x$type_id() == Type["DOUBLE"])
  }
  nse_funcs$is.integer <- function(x) {
    is.integer(x) || (inherits(x, "Expression") && x$type_id() %in% Type[c(
      "UINT8", "INT8", "UINT16", "INT16", "UINT32", "INT32",
      "UINT64", "INT64"
    )])
  }
  nse_funcs$is.integer64 <- function(x) {
    is.integer64(x) || (inherits(x, "Expression") && x$type_id() == Type["INT64"])
  }
  nse_funcs$is.logical <- function(x) {
    is.logical(x) || (inherits(x, "Expression") && x$type_id() == Type["BOOL"])
  }
  nse_funcs$is.factor <- function(x) {
    is.factor(x) || (inherits(x, "Expression") && x$type_id() == Type["DICTIONARY"])
  }
  nse_funcs$is.list <- function(x) {
    is.list(x) || (inherits(x, "Expression") && x$type_id() %in% Type[c(
      "LIST", "FIXED_SIZE_LIST", "LARGE_LIST"
    )])
  }

  # rlang::is_* type functions
  nse_funcs$is_character <- function(x, n = NULL) {
    assert_that(is.null(n))
    nse_funcs$is.character(x)
  }
  nse_funcs$is_double <- function(x, n = NULL, finite = NULL) {
    assert_that(is.null(n) && is.null(finite))
    nse_funcs$is.double(x)
  }
  nse_funcs$is_integer <- function(x, n = NULL) {
    assert_that(is.null(n))
    nse_funcs$is.integer(x)
  }
  nse_funcs$is_list <- function(x, n = NULL) {
    assert_that(is.null(n))
    nse_funcs$is.list(x)
  }
  nse_funcs$is_logical <- function(x, n = NULL) {
    assert_that(is.null(n))
    nse_funcs$is.logical(x)
  }

  # Create a data frame/tibble/struct column
  nse_funcs$tibble <- function(..., .rows = NULL, .name_repair = NULL) {
    if (!is.null(.rows)) arrow_not_supported(".rows")
    if (!is.null(.name_repair)) arrow_not_supported(".name_repair")

    # use dots_list() because this is what tibble() uses to allow the
    # useful shorthand of tibble(col1, col2) -> tibble(col1 = col1, col2 = col2)
    # we have a stronger enforcement of unique names for arguments because
    # it is difficult to replicate the .name_repair semantics and expanding of
    # unnamed data frame arguments in the same way that the tibble() constructor
    # does.
    args <- rlang::dots_list(..., .named = TRUE, .homonyms = "error")

    build_expr(
      "make_struct",
      args = unname(args),
      options = list(field_names = names(args))
    )
  }

  nse_funcs$data.frame <- function(..., row.names = NULL,
                                   check.rows = NULL, check.names = TRUE, fix.empty.names = TRUE,
                                   stringsAsFactors = FALSE) {
    # we need a specific value of stringsAsFactors because the default was
    # TRUE in R <= 3.6
    if (!identical(stringsAsFactors, FALSE)) {
      arrow_not_supported("stringsAsFactors = TRUE")
    }

    # ignore row.names and check.rows with a warning
    if (!is.null(row.names)) arrow_not_supported("row.names")
    if (!is.null(check.rows)) arrow_not_supported("check.rows")

    args <- rlang::dots_list(..., .named = fix.empty.names)
    if (is.null(names(args))) {
      names(args) <- rep("", length(args))
    }

    if (identical(check.names, TRUE)) {
      if (identical(fix.empty.names, TRUE)) {
        names(args) <- make.names(names(args), unique = TRUE)
      } else {
        name_emtpy <- names(args) == ""
        names(args)[!name_emtpy] <- make.names(names(args)[!name_emtpy], unique = TRUE)
      }
    }

    build_expr(
      "make_struct",
      args = unname(args),
      options = list(field_names = names(args))
    )
  }
}

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

register_math_translations <- function() {

  nse_funcs$log <- nse_funcs$logb <- function(x, base = exp(1)) {
    # like other binary functions, either `x` or `base` can be Expression or double(1)
    if (is.numeric(x) && length(x) == 1) {
      x <- Expression$scalar(x)
    } else if (!inherits(x, "Expression")) {
      arrow_not_supported("x must be a column or a length-1 numeric; other values")
    }

    # handle `base` differently because we use the simpler ln, log2, and log10
    # functions for specific scalar base values
    if (inherits(base, "Expression")) {
      return(Expression$create("logb_checked", x, base))
    }

    if (!is.numeric(base) || length(base) != 1) {
      arrow_not_supported("base must be a column or a length-1 numeric; other values")
    }

    if (base == exp(1)) {
      return(Expression$create("ln_checked", x))
    }

    if (base == 2) {
      return(Expression$create("log2_checked", x))
    }

    if (base == 10) {
      return(Expression$create("log10_checked", x))
    }

    Expression$create("logb_checked", x, Expression$scalar(base))
  }


  nse_funcs$pmin <- function(..., na.rm = FALSE) {
    build_expr(
      "min_element_wise",
      ...,
      options = list(skip_nulls = na.rm)
    )
  }

  nse_funcs$pmax <- function(..., na.rm = FALSE) {
    build_expr(
      "max_element_wise",
      ...,
      options = list(skip_nulls = na.rm)
    )
  }

  nse_funcs$trunc <- function(x, ...) {
    # accepts and ignores ... for consistency with base::trunc()
    build_expr("trunc", x)
  }

  nse_funcs$round <- function(x, digits = 0) {
    build_expr(
      "round",
      x,
      options = list(ndigits = digits, round_mode = RoundMode$HALF_TO_EVEN)
    )
  }

}

# Aggregation functions
# These all return a list of:
# @param fun string function name
# @param data Expression (these are all currently a single field)
# @param options list of function options, as passed to call_function
# For group-by aggregation, `hash_` gets prepended to the function name.
# So to see a list of available hash aggregation functions,
# you can use list_compute_functions("^hash_")


ensure_one_arg <- function(args, fun) {
  if (length(args) == 0) {
    arrow_not_supported(paste0(fun, "() with 0 arguments"))
  } else if (length(args) > 1) {
    arrow_not_supported(paste0("Multiple arguments to ", fun, "()"))
  }
  args[[1]]
}

agg_fun_output_type <- function(fun, input_type, hash) {
  # These are quick and dirty heuristics.
  if (fun %in% c("any", "all")) {
    bool()
  } else if (fun %in% "sum") {
    # It may upcast to a bigger type but this is close enough
    input_type
  } else if (fun %in% c("mean", "stddev", "variance", "approximate_median")) {
    float64()
  } else if (fun %in% "tdigest") {
    if (hash) {
      fixed_size_list_of(float64(), 1L)
    } else {
      float64()
    }
  } else {
    # Just so things don't error, assume the resulting type is the same
    input_type
  }
}

register_aggregate_translations <- function() {

  agg_funcs$sum <- function(..., na.rm = FALSE) {
    list(
      fun = "sum",
      data = ensure_one_arg(list2(...), "sum"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  }
  agg_funcs$any <- function(..., na.rm = FALSE) {
    list(
      fun = "any",
      data = ensure_one_arg(list2(...), "any"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  }
  agg_funcs$all <- function(..., na.rm = FALSE) {
    list(
      fun = "all",
      data = ensure_one_arg(list2(...), "all"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  }
  agg_funcs$mean <- function(x, na.rm = FALSE) {
    list(
      fun = "mean",
      data = x,
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  }
  agg_funcs$sd <- function(x, na.rm = FALSE, ddof = 1) {
    list(
      fun = "stddev",
      data = x,
      options = list(skip_nulls = na.rm, min_count = 0L, ddof = ddof)
    )
  }
  agg_funcs$var <- function(x, na.rm = FALSE, ddof = 1) {
    list(
      fun = "variance",
      data = x,
      options = list(skip_nulls = na.rm, min_count = 0L, ddof = ddof)
    )
  }
  agg_funcs$quantile <- function(x, probs, na.rm = FALSE) {
    if (length(probs) != 1) {
      arrow_not_supported("quantile() with length(probs) != 1")
    }
    # TODO: Bind to the Arrow function that returns an exact quantile and remove
    # this warning (ARROW-14021)
    warn(
      "quantile() currently returns an approximate quantile in Arrow",
      .frequency = ifelse(is_interactive(), "once", "always"),
      .frequency_id = "arrow.quantile.approximate"
    )
    list(
      fun = "tdigest",
      data = x,
      options = list(skip_nulls = na.rm, q = probs)
    )
  }
  agg_funcs$median <- function(x, na.rm = FALSE) {
    # TODO: Bind to the Arrow function that returns an exact median and remove
    # this warning (ARROW-14021)
    warn(
      "median() currently returns an approximate median in Arrow",
      .frequency = ifelse(is_interactive(), "once", "always"),
      .frequency_id = "arrow.median.approximate"
    )
    list(
      fun = "approximate_median",
      data = x,
      options = list(skip_nulls = na.rm)
    )
  }
  agg_funcs$n_distinct <- function(..., na.rm = FALSE) {
    list(
      fun = "count_distinct",
      data = ensure_one_arg(list2(...), "n_distinct"),
      options = list(na.rm = na.rm)
    )
  }
  agg_funcs$n <- function() {
    list(
      fun = "sum",
      data = Expression$scalar(1L),
      options = list()
    )
  }
  agg_funcs$min <- function(..., na.rm = FALSE) {
    list(
      fun = "min",
      data = ensure_one_arg(list2(...), "min"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  }
  agg_funcs$max <- function(..., na.rm = FALSE) {
    list(
      fun = "max",
      data = ensure_one_arg(list2(...), "max"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  }
}
