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

#' @include arrowExports.R

.unary_function_map <- list(
  # NOTE: Each of the R functions mapped here takes exactly *one* argument, maps
  # *directly* to an Arrow C++ compute kernel, and does not require any
  # non-default options to be specified. More complex R function mappings are
  # defined in dplyr-functions.R.

  # functions are arranged alphabetically by name within categories

  # arithmetic functions
  "abs" = "abs_checked",
  "ceiling" = "ceil",
  "floor" = "floor",
  "log10" = "log10_checked",
  "log1p" = "log1p_checked",
  "log2" = "log2_checked",
  "sign" = "sign",
  # trunc is defined in dplyr-functions.R

  # trigonometric functions
  "acos" = "acos_checked",
  "asin" = "asin_checked",
  "cos" = "cos_checked",
  "sin" = "sin_checked",
  "tan" = "tan_checked",

  # logical functions
  "!" = "invert",

  # string functions
  # nchar is defined in dplyr-functions.R
  "str_length" = "utf8_length",
  # str_pad is defined in dplyr-functions.R
  # str_sub is defined in dplyr-functions.R
  "str_to_lower" = "utf8_lower",
  "str_to_upper" = "utf8_upper",
  # str_trim is defined in dplyr-functions.R
  "stri_reverse" = "utf8_reverse",
  # substr is defined in dplyr-functions.R
  # substring is defined in dplyr-functions.R
  "tolower" = "utf8_lower",
  "toupper" = "utf8_upper",

  # date and time functions
  "day" = "day",
  "hour" = "hour",
  "isoweek" = "iso_week",
  "isoyear" = "iso_year",
  "minute" = "minute",
  "month" = "month",
  "quarter" = "quarter",
  # second is defined in dplyr-functions.R
  # wday is defined in dplyr-functions.R
  "yday" = "day_of_year",
  "year" = "year",

  # type conversion functions
  "as.factor" = "dictionary_encode"
)

.binary_function_map <- list(
  # NOTE: Each of the R functions/operators mapped here takes exactly *two*
  # arguments. Most map *directly* to an Arrow C++ compute kernel and require no
  # non-default options, but some are modified by build_expr(). More complex R
  # function/operator mappings are defined in dplyr-functions.R.

  "==" = "equal",
  "!=" = "not_equal",
  ">" = "greater",
  ">=" = "greater_equal",
  "<" = "less",
  "<=" = "less_equal",
  "&" = "and_kleene",
  "|" = "or_kleene",
  "+" = "add_checked",
  "-" = "subtract_checked",
  "*" = "multiply_checked",
  "/" = "divide_checked",
  "%/%" = "divide_checked",
  # we don't actually use divide_checked with `%%`, rather it is rewritten to
  # use %/% above.
  "%%" = "divide_checked",
  "^" = "power_checked",
  "%in%" = "is_in_meta_binary"
)

.array_function_map <- c(.unary_function_map, .binary_function_map)

#' Arrow expressions
#'
#' @description
#' `Expression`s are used to define filter logic for passing to a [Dataset]
#' [Scanner].
#'
#' `Expression$scalar(x)` constructs an `Expression` which always evaluates to
#' the provided scalar (length-1) R value.
#'
#' `Expression$field_ref(name)` is used to construct an `Expression` which
#' evaluates to the named column in the `Dataset` against which it is evaluated.
#'
#' `Expression$create(function_name, ..., options)` builds a function-call
#' `Expression` containing one or more `Expression`s.
#' @name Expression
#' @rdname Expression
#' @export
Expression <- R6Class("Expression", inherit = ArrowObject,
  public = list(
    ToString = function() compute___expr__ToString(self),
    # TODO: Implement type determination without storing
    # schemas in Expression objects (ARROW-13186)
    schema = NULL,
    type = function(schema = self$schema) {
      assert_that(!is.null(schema))
      compute___expr__type(self, schema)
    },
    type_id = function(schema = self$schema) {
      assert_that(!is.null(schema))
      compute___expr__type_id(self, schema)
    },
    cast = function(to_type, safe = TRUE, ...) {
      opts <- list(
        to_type = to_type,
        allow_int_overflow = !safe,
        allow_time_truncate = !safe,
        allow_float_truncate = !safe
      )
      Expression$create("cast", self, options = modifyList(opts, list(...)))
    }
  ),
  active = list(
    field_name = function() compute___expr__get_field_ref_name(self)
  )
)
Expression$create <- function(function_name,
                              ...,
                              args = list(...),
                              options = empty_named_list()) {
  assert_that(is.string(function_name))
  assert_that(is_list_of(args, "Expression"), msg = "Expression arguments must be Expression objects")
  expr <- compute___expr__call(function_name, args, options)
  expr$schema <- unify_schemas(schemas = lapply(args, function(x) x$schema))
  expr
}

Expression$field_ref <- function(name) {
  assert_that(is.string(name))
  compute___expr__field_ref(name)
}
Expression$scalar <- function(x) {
  expr <- compute___expr__scalar(Scalar$create(x))
  expr$schema <- schema()
  expr
}

# Wrapper around Expression$create that:
# (1) maps R function names to Arrow C++ compute ("/" --> "divide_checked")
# (2) wraps R input args as Array or Scalar
build_expr <- function(FUN,
                       ...,
                       args = list(...),
                       options = empty_named_list()) {
  if (FUN == "-" && length(args) == 1L) {
    if (inherits(args[[1]], c("ArrowObject", "Expression"))) {
      return(build_expr("negate_checked", args[[1]]))
    } else {
      return(-args[[1]])
    }
  }
  if (FUN == "%in%") {
    # Special-case %in%, which is different from the Array function name
    expr <- Expression$create("is_in", args[[1]],
      options = list(
        # If args[[2]] is already an Arrow object (like a scalar),
        # this wouldn't work
        value_set = Array$create(args[[2]]),
        skip_nulls = TRUE
      )
    )
  } else {
    args <- lapply(args, function(x) {
      if (!inherits(x, "Expression")) {
        x <- Expression$scalar(x)
      }
      x
    })

    # In Arrow, "divide" is one function, which does integer division on
    # integer inputs and floating-point division on floats
    if (FUN == "/") {
      # TODO: omg so many ways it's wrong to assume these types
      args <- lapply(args, function(x) x$cast(float64()))
    } else if (FUN == "%/%") {
      # In R, integer division works like floor(float division)
      out <- build_expr("/", args = args)
      return(out$cast(int32(), allow_float_truncate = TRUE))
    } else if (FUN == "%%") {
      return(args[[1]] - args[[2]] * ( args[[1]] %/% args[[2]] ))
    }

    expr <- Expression$create(.array_function_map[[FUN]] %||% FUN, args = args, options = options)
  }
  expr
}

#' @export
Ops.Expression <- function(e1, e2) {
  if (.Generic == "!") {
    build_expr(.Generic, e1)
  } else {
    build_expr(.Generic, e1, e2)
  }
}

#' @export
is.na.Expression <- function(x) {
  if (!is.null(x$schema) && x$type_id() %in% TYPES_WITH_NAN) {
    # TODO: if an option is added to the is_null kernel to treat NaN as NA,
    # use that to simplify the code here (ARROW-13367)
    Expression$create("is_nan", x) | build_expr("is_null", x)
  } else {
    Expression$create("is_null", x)
  }
}
