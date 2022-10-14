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
  "base::abs" = "abs_checked",
  "base::ceiling" = "ceil",
  "base::floor" = "floor",
  "base::log10" = "log10_checked",
  "base::log1p" = "log1p_checked",
  "base::log2" = "log2_checked",
  "base::sign" = "sign",
  # trunc is defined in dplyr-functions.R

  # trigonometric functions
  "base::acos" = "acos_checked",
  "base::asin" = "asin_checked",
  "base::cos" = "cos_checked",
  "base::sin" = "sin_checked",
  "base::tan" = "tan_checked",

  # logical functions
  "!" = "invert",

  # string functions
  # nchar is defined in dplyr-functions.R
  "stringr::str_length" = "utf8_length",
  # str_pad is defined in dplyr-functions.R
  # str_sub is defined in dplyr-functions.R
  # str_to_lower is defined in dplyr-functions.R
  # str_to_title is defined in dplyr-functions.R
  # str_to_upper is defined in dplyr-functions.R
  # str_trim is defined in dplyr-functions.R
  "stringi::stri_reverse" = "utf8_reverse",
  # substr is defined in dplyr-functions.R
  # substring is defined in dplyr-functions.R
  "base::tolower" = "utf8_lower",
  "base::toupper" = "utf8_upper",

  # date and time functions
  "lubridate::day" = "day",
  "lubridate::dst" = "is_dst",
  "lubridate::hour" = "hour",
  "lubridate::isoweek" = "iso_week",
  "lubridate::epiweek" = "us_week",
  "lubridate::isoyear" = "iso_year",
  "lubridate::epiyear" = "us_year",
  "lubridate::minute" = "minute",
  "lubridate::quarter" = "quarter",
  # second is defined in dplyr-functions.R
  # wday is defined in dplyr-functions.R
  "lubridate::mday" = "day",
  "lubridate::yday" = "day_of_year",
  "lubridate::year" = "year",
  "lubridate::leap_year" = "is_leap_year"
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
  "/" = "divide",
  "%/%" = "divide_checked",
  # we don't actually use divide_checked with `%%`, rather it is rewritten to
  # use `%/%` above.
  "%%" = "divide_checked",
  "^" = "power_checked",
  "%in%" = "is_in_meta_binary",
  "base::strrep" = "binary_repeat",
  "stringr::str_dup" = "binary_repeat"
)

.array_function_map <- c(.unary_function_map, .binary_function_map)

register_bindings_array_function_map <- function() {
  # use a function to generate the binding so that `operator` persists
  # beyond execution time (another option would be to use quasiquotation
  # and unquote `operator` directly into the function expression)
  array_function_map_factory <- function(operator) {
    force(operator)
    function(...) build_expr(operator, ...)
  }

  for (name in names(.array_function_map)) {
    register_binding(name, array_function_map_factory(name))
  }
}

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
Expression <- R6Class("Expression",
  inherit = ArrowObject,
  public = list(
    ToString = function() compute___expr__ToString(self),
    Equals = function(other, ...) {
      inherits(other, "Expression") && compute___expr__equals(self, other)
    },
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
      opts <- cast_options(safe, ...)
      opts$to_type <- as_type(to_type)
      Expression$create("cast", self, options = opts)
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
  if (length(args)) {
    expr$schema <- unify_schemas(schemas = lapply(args, function(x) x$schema))
  } else {
    # TODO: this shouldn't be necessary
    expr$schema <- schema()
  }
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

      # integer output only for all integer input
      int_type_ids <- Type[toupper(INTEGER_TYPES)]
      numerator_is_int <- args[[1]]$type_id() %in% int_type_ids
      denominator_is_int <- args[[2]]$type_id() %in% int_type_ids

      if (numerator_is_int && denominator_is_int) {
        out_float <- build_expr(
          "if_else",
          build_expr("equal", args[[2]], 0L),
          Scalar$create(NA_integer_),
          build_expr("floor", out)
        )
        return(out_float$cast(args[[1]]$type()))
      } else {
        return(build_expr("floor", out))
      }
    } else if (FUN == "%%") {
      return(args[[1]] - args[[2]] * (args[[1]] %/% args[[2]]))
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
  Expression$create("is_null", x, options = list(nan_is_null = TRUE))
}
