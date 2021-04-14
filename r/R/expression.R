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

array_expression <- function(FUN,
                             ...,
                             args = list(...),
                             options = empty_named_list()) {
  structure(
    list(
      fun = FUN,
      args = args,
      options = options
    ),
    class = "array_expression"
  )
}

#' @export
Ops.ArrowDatum <- function(e1, e2) {
  if (.Generic == "!") {
    eval_array_expression(build_array_expression(.Generic, e1))
  } else if (.Generic %in% names(.array_function_map)) {
    eval_array_expression(build_array_expression(.Generic, e1, e2))
  } else {
    stop(paste0("Unsupported operation on `", class(e1)[1L], "` : "), .Generic, call. = FALSE)
  }
}

#' @export
Ops.array_expression <- function(e1, e2) {
  if (.Generic == "!") {
    build_array_expression(.Generic, e1)
  } else {
    build_array_expression(.Generic, e1, e2)
  }
}

build_array_expression <- function(FUN,
                                   ...,
                                   args = list(...),
                                   options = empty_named_list()) {
  if (FUN == "-" && length(args) == 1L) {
    # Unary -, i.e. just make it negative, and somehow this works
    if (inherits(args[[1]], c("ArrowObject", "array_expression"))) {
      # Make it be 0 - arg
      # TODO(ARROW-11950): do this in C++ compute
      args <- list(0L, args[[1]])
    } else {
      # Somehow this works
      return(-args[[1]])
    }
  }
  args <- lapply(args, .wrap_arrow, FUN)

  # In Arrow, "divide" is one function, which does integer division on
  # integer inputs and floating-point division on floats
  if (FUN == "/") {
    # TODO: omg so many ways it's wrong to assume these types
    args <- lapply(args, cast_array_expression, float64())
  } else if (FUN == "%/%") {
    # In R, integer division works like floor(float division)
    out <- build_array_expression("/", args = args, options = options)
    return(cast_array_expression(out, int32(), allow_float_truncate = TRUE))
  } else if (FUN == "%%") {
    # {e1 - e2 * ( e1 %/% e2 )}
    # ^^^ form doesn't work because Ops.Array evaluates eagerly,
    # but we can build that up
    quotient <- build_array_expression("%/%", args = args)
    base <- build_array_expression("*", quotient, args[[2]])
    # this cast is to ensure that the result of this and e1 are the same
    # (autocasting only applies to scalars)
    base <- cast_array_expression(base, args[[1]]$type)
    return(build_array_expression("-", args[[1]], base))
  }

  array_expression(.array_function_map[[FUN]] %||% FUN, args = args, options = options)
}

cast_array_expression <- function(x, to_type, safe = TRUE, ...) {
  opts <- list(
    to_type = to_type,
    allow_int_overflow = !safe,
    allow_time_truncate = !safe,
    allow_float_truncate = !safe
  )
  array_expression("cast", x, options = modifyList(opts, list(...)))
}

.wrap_arrow <- function(arg, fun) {
  if (!inherits(arg, c("ArrowObject", "array_expression"))) {
    # TODO: Array$create if lengths are equal?
    # TODO: these kernels should autocast like the dataset ones do (e.g. int vs. float)
    if (fun == "%in%") {
      arg <- Array$create(arg)
    } else {
      arg <- Scalar$create(arg)
    }
  }
  arg
}

.unary_function_map <- list(
  "!" = "invert",
  "as.factor" = "dictionary_encode",
  "is.na" = "is_null",
  "is.nan" = "is_nan",
  # nchar is defined in dplyr.R because it is more complex
  # "nchar" = "utf8_length",
  "tolower" = "utf8_lower",
  "toupper" = "utf8_upper",
  # stringr spellings of those
  "str_length" = "utf8_length",
  "str_to_lower" = "utf8_lower",
  "str_to_upper" = "utf8_upper"
  # str_trim is defined in dplyr.R
)

.binary_function_map <- list(
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

eval_array_expression <- function(x, data = NULL) {
  if (!is.null(data)) {
    x <- bind_array_refs(x, data)
  }
  if (!inherits(x, "array_expression")) {
    # Nothing to evaluate
    return(x)
  }
  x$args <- lapply(x$args, function (a) {
    if (inherits(a, "array_expression")) {
      eval_array_expression(a)
    } else {
      a
    }
  })
  if (x$fun == "is_in_meta_binary" && inherits(x$args[[2]], "Scalar")) {
    x$args[[2]] <- Array$create(x$args[[2]])
  }
  call_function(x$fun, args = x$args, options = x$options %||% empty_named_list())
}

find_array_refs <- function(x) {
  if (identical(x$fun, "array_ref")) {
    out <- x$args$field_name
  } else {
    out <- lapply(x$args, find_array_refs)
  }
  unlist(out)
}

# Take an array_expression and replace array_refs with arrays/chunkedarrays from data
bind_array_refs <- function(x, data) {
  if (inherits(x, "array_expression")) {
    if (identical(x$fun, "array_ref")) {
      x <- data[[x$args$field_name]]
    } else {
      x$args <- lapply(x$args, bind_array_refs, data)
    }
  }
  x
}

#' @export
is.na.array_expression <- function(x) array_expression("is.na", x)

#' @export
as.vector.array_expression <- function(x, ...) {
  as.vector(eval_array_expression(x))
}

#' @export
print.array_expression <- function(x, ...) {
  cat(.format_array_expression(x), "\n", sep = "")
  invisible(x)
}

.format_array_expression <- function(x) {
  printed_args <- map_chr(x$args, function(arg) {
    if (inherits(arg, "Scalar")) {
      deparse(as.vector(arg))
    } else if (inherits(arg, "ArrowObject")) {
      paste0("<", class(arg)[1], ">")
    } else if (inherits(arg, "array_expression")) {
      .format_array_expression(arg)
    } else {
      # Should not happen
      deparse(arg)
    }
  })
  if (identical(x$fun, "array_ref")) {
    x$args$field_name
  } else {
    # Prune this for readability
    function_name <- sub("_kleene", "", x$fun)
    paste0(function_name, "(", paste(printed_args, collapse = ", "), ")")
  }
}

###########

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
    ToString = function() dataset___expr__ToString(self),
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
    field_name = function() dataset___expr__get_field_ref_name(self)
  )
)
Expression$create <- function(function_name,
                              ...,
                              args = list(...),
                              options = empty_named_list()) {
  assert_that(is.string(function_name))
  dataset___expr__call(function_name, args, options)
}
Expression$field_ref <- function(name) {
  assert_that(is.string(name))
  dataset___expr__field_ref(name)
}
Expression$scalar <- function(x) {
  dataset___expr__scalar(Scalar$create(x))
}

build_dataset_expression <- function(FUN,
                                     ...,
                                     args = list(...),
                                     options = empty_named_list()) {
  if (FUN == "-" && length(args) == 1L) {
    # Unary -, i.e. make it negative
    if (inherits(args[[1]], c("ArrowObject", "Expression"))) {
      # TODO(ARROW-11950): do this in C++ compute
      args <- list(0L, args[[1]])
    } else {
      # Somehow this just works
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
      out <- build_dataset_expression("/", args = args)
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
    build_dataset_expression(.Generic, e1)
  } else {
    build_dataset_expression(.Generic, e1, e2)
  }
}

#' @export
is.na.Expression <- function(x) Expression$create("is_null", x)
