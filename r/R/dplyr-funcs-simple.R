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


.unary_function_map <- list(
  # NOTE: Each of the R functions mapped here takes exactly *one* argument, maps
  # *directly* to an Arrow C++ compute kernel, and does not require any
  # non-default options to be specified. More complex R function mappings are
  # defined in dplyr-funcs-*.R.

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

.operator_map <- list(
  # NOTE: Each of the R functions/operators mapped here takes exactly *two*
  # arguments. Most map *directly* to an Arrow C++ compute kernel and require no
  # non-default options, but some are modified in Expression$op().
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
  # use `%/%` in Expression$op().
  "%%" = "divide_checked",
  "^" = "power_checked"
)

.array_function_map <- c(.unary_function_map, .operator_map)

register_bindings_array_function_map <- function() {
  # use a function to generate the binding so that `operator` persists
  # beyond execution time (another option would be to use quasiquotation
  # and unquote `operator` directly into the function expression)
  unary_factory <- function(operator) {
    force(operator)
    function(...) Expression$create(operator, ...)
  }
  for (name in names(.unary_function_map)) {
    register_binding(name, unary_factory(.unary_function_map[[name]]))
  }

  # These go through Expression$op() to align types
  operator_factory <- function(operator) {
    force(operator)
    function(...) Expression$op(operator, ...)
  }

  for (name in names(.operator_map)) {
    register_binding(name, operator_factory(name))
  }
}

# This function looks to see if there is a common type among any Expressions in
# the args list, and if so, it tries to cast any Scalars to match that type.
# This is so field<int16> + 1<float64> doesn't result in all values of the field
# being upcast to float64 just because R natively does not have int16.
#
# Logical and arithmetic operators go through here, as do the yes/no cases of
# if_else. You can use this in your dplyr function bindings if it's appropriate,
# but be sure that it makes sense.
cast_scalars_to_common_type <- function(args) {
  is_expr <- map_lgl(args, ~ inherits(., "Expression"))
  if (all(is_expr)) {
    # No wrapping is required
    return(args)
  }

  args[!is_expr] <- lapply(args[!is_expr], Scalar$create)

  if (any(is_expr)) {
    tryCatch(
      {
        # If the Expression has no Schema embedded, we cannot resolve its
        # type here, so this will error, hence the try() wrapping it
        # This will also error if length(args[is_expr]) == 0, or
        # if there are multiple exprs that do not share a common type.
        to_type <- common_type(args[is_expr])
        # Try casting to this type, but if the cast fails,
        # we'll just keep the original
        args[!is_expr] <- lapply(args[!is_expr], cast_or_parse, type = to_type)
      },
      error = function(e) {
        # We do want to error for some types of casting errors
        if (inherits(e, "arrow_error_implicit_cast")) {
          abort("Cast error", parent = e)
        }

        # Other cast errors we ignore and let Arrow handle at collect()
      }
    )
  }

  args[!is_expr] <- lapply(args[!is_expr], Expression$scalar)
  args
}

common_type <- function(exprs) {
  types <- map(exprs, ~ .$type())
  first_type <- types[[1]]
  if (length(types) == 1 || all(map_lgl(types, ~ .$Equals(first_type)))) {
    # Functions (in our tests) that have multiple exprs to check:
    # * pmin/pmax
    return(first_type)
  }
  stop("There is no common type in these expressions")
}

cast_or_parse <- function(x, type) {
  to_type_id <- type$id
  if (to_type_id %in% c(Type[["DECIMAL128"]], Type[["DECIMAL256"]])) {
    # TODO: determine the minimum size of decimal (or integer) required to
    # accommodate x
    # We would like to keep calculations on decimal if that's what the data has
    # so that we don't lose precision. However, there are some limitations
    # today, so it makes sense to keep x as double (which is probably is from R)
    # and let Acero cast the decimal to double to compute.
    # You can specify in your query that x should be decimal or integer if you
    # know it to be safe.
    # * ARROW-17601: multiply(decimal, decimal) can fail to make output type
    return(x)
  }

  # For most types, just cast.
  # For string -> date/time, we need to call a parsing function.
  # In dplyr 1.1.0, vctrs::vec_ptype2() rules are used, which are mostly
  # the same as Arrow rules except that implicit conversion from string to
  # numeric is no longer supported.
  if (x$type_id() %in% c(Type[["STRING"]], Type[["LARGE_STRING"]])) {
    if (to_type_id %in% c(Type[["DATE32"]], Type[["DATE64"]])) {
      x <- call_function(
        "strptime",
        x,
        options = list(format = "%Y-%m-%d", unit = 0L)
      )
    } else if (to_type_id == Type[["TIMESTAMP"]]) {
      x <- call_function(
        "strptime",
        x,
        options = list(format = "%Y-%m-%d %H:%M:%S", unit = 1L)
      )
      # R assumes timestamps without timezone specified are
      # local timezone while Arrow assumes UTC. For consistency
      # with R behavior, specify local timezone here.
      x <- call_function(
        "assume_timezone",
        x,
        options = list(timezone = Sys.timezone())
      )
    } else if (to_type_id %in% unlist(TYPES_NUMERIC)) {
      abort(
        sprintf(
          "Implicit cast from %s to %s is not supported",
          x$type$ToString(),
          type$ToString()
        ),
        class = "arrow_error_implicit_cast"
      )
    }
  }
  x$cast(type)
}
