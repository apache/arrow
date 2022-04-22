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

# Split up into several register functions by category to satisfy the linter
register_bindings_type <- function() {
  register_bindings_type_cast()
  register_bindings_type_inspect()
  register_bindings_type_elementwise()
  register_bindings_type_format()
}

register_bindings_type_cast <- function() {
  register_binding("cast", function(x, target_type, safe = TRUE, ...) {
    opts <- cast_options(safe, ...)
    opts$to_type <- as_type(target_type)
    Expression$create("cast", x, options = opts)
  })

  register_binding("dictionary_encode", function(x,
                                                     null_encoding_behavior = c("mask", "encode")) {
    behavior <- toupper(match.arg(null_encoding_behavior))
    null_encoding_behavior <- NullEncodingBehavior[[behavior]]
    Expression$create(
      "dictionary_encode",
      x,
      options = list(null_encoding_behavior = null_encoding_behavior)
    )
  })

  # as.* type casting functions
  # as.factor() is mapped in expression.R
  register_binding("as.character", function(x) {
    build_expr("cast", x, options = cast_options(to_type = string()))
  })
  register_binding("as.double", function(x) {
    build_expr("cast", x, options = cast_options(to_type = float64()))
  })
  register_binding("as.integer", function(x) {
    build_expr(
      "cast",
      x,
      options = cast_options(
        to_type = int32(),
        allow_float_truncate = TRUE,
        allow_decimal_truncate = TRUE
      )
    )
  })
  register_binding("as.integer64", function(x) {
    build_expr(
      "cast",
      x,
      options = cast_options(
        to_type = int64(),
        allow_float_truncate = TRUE,
        allow_decimal_truncate = TRUE
      )
    )
  })
  register_binding("as.logical", function(x) {
    build_expr("cast", x, options = cast_options(to_type = boolean()))
  })
  register_binding("as.numeric", function(x) {
    build_expr("cast", x, options = cast_options(to_type = float64()))
  })
  register_binding("as.Date", function(x,
                                       format = NULL,
                                       tryFormats = "%Y-%m-%d",
                                       origin = "1970-01-01",
                                       tz = "UTC") {
    # base::as.Date() and lubridate::as_date() differ in the way they use the
    # `tz` argument. Both cast to the desired timezone, if present. The
    # difference appears when the `tz` argument is not set: `as.Date()` uses the
    # default value ("UTC"), while `as_date()` keeps the original attribute
    # => we only cast when we want the behaviour of the base version or when
    # `tz` is set (i.e. not NULL)
    if (call_binding("is.POSIXct", x)) {
      x <- build_expr("cast", x, options = cast_options(to_type = timestamp(timezone = tz)))
    }

    binding_as_date(
      x = x,
      format = format,
      tryFormats = tryFormats,
      origin = origin
    )
  })

  register_binding("as_date", function(x,
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
      x <- build_expr("cast", x, options = cast_options(to_type = timestamp(timezone = tz)))
    }
    binding_as_date(
      x = x,
      format = format,
      origin = origin
    )
  })

  register_binding("as_datetime", function(x,
                                           origin = "1970-01-01",
                                           tz = "UTC",
                                           format = NULL) {
    if (call_binding("is.numeric", x)) {
      delta <- call_binding("difftime", origin, "1970-01-01")
      delta <- build_expr("cast", delta, options = cast_options(to_type = int64()))
      x <- build_expr("cast", x, options = cast_options(to_type = int64()))
      x <- build_expr("+", x, delta)
    }

    if (call_binding("is.character", x) && !is.null(format)) {
      # unit = 0L is the identifier for seconds in valid_time32_units
      x <- build_expr(
        "strptime",
        x,
        options = list(format = format, unit = 0L, error_is_null = TRUE)
      )
    }
    output <- build_expr("cast", x, options = cast_options(to_type = timestamp()))
    build_expr("assume_timezone", output, options = list(timezone = tz))
  })

  register_binding("is", function(object, class2) {
    if (is.string(class2)) {
      switch(class2,
        # for R data types, pass off to is.*() functions
        character = call_binding("is.character", object),
        numeric = call_binding("is.numeric", object),
        integer = call_binding("is.integer", object),
        integer64 = call_binding("is.integer64", object),
        logical = call_binding("is.logical", object),
        factor = call_binding("is.factor", object),
        list = call_binding("is.list", object),
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
  })

  # Create a data frame/tibble/struct column
  register_binding("tibble", function(..., .rows = NULL, .name_repair = NULL) {
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
  })

  register_binding("data.frame", function(..., row.names = NULL,
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
  })
}

register_bindings_type_inspect <- function() {
  # is.* type functions
  register_binding("is.character", function(x) {
    is.character(x) || (inherits(x, "Expression") &&
                          x$type_id() %in% Type[c("STRING", "LARGE_STRING")])
  })
  register_binding("is.numeric", function(x) {
    is.numeric(x) || (inherits(x, "Expression") && x$type_id() %in% Type[c(
      "UINT8", "INT8", "UINT16", "INT16", "UINT32", "INT32",
      "UINT64", "INT64", "HALF_FLOAT", "FLOAT", "DOUBLE",
      "DECIMAL128", "DECIMAL256"
    )])
  })
  register_binding("is.double", function(x) {
    is.double(x) || (inherits(x, "Expression") && x$type_id() == Type["DOUBLE"])
  })
  register_binding("is.integer", function(x) {
    is.integer(x) || (inherits(x, "Expression") && x$type_id() %in% Type[c(
      "UINT8", "INT8", "UINT16", "INT16", "UINT32", "INT32",
      "UINT64", "INT64"
    )])
  })
  register_binding("is.integer64", function(x) {
    inherits(x, "integer64") || (inherits(x, "Expression") && x$type_id() == Type["INT64"])
  })
  register_binding("is.logical", function(x) {
    is.logical(x) || (inherits(x, "Expression") && x$type_id() == Type["BOOL"])
  })
  register_binding("is.factor", function(x) {
    is.factor(x) || (inherits(x, "Expression") && x$type_id() == Type["DICTIONARY"])
  })
  register_binding("is.list", function(x) {
    is.list(x) || (inherits(x, "Expression") && x$type_id() %in% Type[c(
      "LIST", "FIXED_SIZE_LIST", "LARGE_LIST"
    )])
  })

  # rlang::is_* type functions
  register_binding("is_character", function(x, n = NULL) {
    assert_that(is.null(n))
    call_binding("is.character", x)
  })
  register_binding("is_double", function(x, n = NULL, finite = NULL) {
    assert_that(is.null(n) && is.null(finite))
    call_binding("is.double", x)
  })
  register_binding("is_integer", function(x, n = NULL) {
    assert_that(is.null(n))
    call_binding("is.integer", x)
  })
  register_binding("is_list", function(x, n = NULL) {
    assert_that(is.null(n))
    call_binding("is.list", x)
  })
  register_binding("is_logical", function(x, n = NULL) {
    assert_that(is.null(n))
    call_binding("is.logical", x)
  })
}

register_bindings_type_elementwise <- function() {
  register_binding("is.na", function(x) {
    build_expr("is_null", x, options = list(nan_is_null = TRUE))
  })

  register_binding("is.nan", function(x) {
    if (is.double(x) || (inherits(x, "Expression") &&
                         x$type_id() %in% TYPES_WITH_NAN)) {
      # TODO: if an option is added to the is_nan kernel to treat NA as NaN,
      # use that to simplify the code here (ARROW-13366)
      build_expr("is_nan", x) & build_expr("is_valid", x)
    } else {
      Expression$scalar(FALSE)
    }
  })

  register_binding("between", function(x, left, right) {
    x >= left & x <= right
  })

  register_binding("is.finite", function(x) {
    is_fin <- Expression$create("is_finite", x)
    # for compatibility with base::is.finite(), return FALSE for NA_real_
    is_fin & !call_binding("is.na", is_fin)
  })

  register_binding("is.infinite", function(x) {
    is_inf <- Expression$create("is_inf", x)
    # for compatibility with base::is.infinite(), return FALSE for NA_real_
    is_inf & !call_binding("is.na", is_inf)
  })
}

register_bindings_type_format <- function() {
  register_binding("format", function(x, ...) {
    # We use R's format if we get a single R object here since we don't (yet)
    # support all of the possible options for casting to string
    if (!inherits(x, "Expression")) {
      return(format(x, ...))
    }

    if (inherits(x, "Expression") &&
        x$type_id() %in% Type[c("TIMESTAMP", "DATE32", "DATE64")]) {
      binding_format_datetime(x, ...)
    } else {
      build_expr("cast", x, options = cast_options(to_type = string()))
    }
  })
}
