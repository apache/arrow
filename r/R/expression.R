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
Ops.Array <- function(e1, e2) {
  if (.Generic %in% names(.array_function_map)) {
    expr <- build_array_expression(.Generic, e1, e2)
    eval_array_expression(expr)
  } else {
    stop(paste0("Unsupported operation on `", class(e1)[1L], "` : "), .Generic, call. = FALSE)
  }
}

#' @export
Ops.ChunkedArray <- Ops.Array

#' @export
Ops.array_expression <- function(e1, e2) {
  if (.Generic == "!") {
    build_array_expression(.Generic, e1)
  } else {
    build_array_expression(.Generic, e1, e2)
  }
}

build_array_expression <- function(.Generic, e1, e2, ...) {
  if (.Generic %in% names(.unary_function_map)) {
    expr <- array_expression(.unary_function_map[[.Generic]], e1)
  } else {
    e1 <- .wrap_arrow(e1, .Generic)
    e2 <- .wrap_arrow(e2, .Generic)

    # In Arrow, "divide" is one function, which does integer division on
    # integer inputs and floating-point division on floats
    if (.Generic == "/") {
      # TODO: omg so many ways it's wrong to assume these types
      e1 <- cast_array_expression(e1, float64())
      e2 <- cast_array_expression(e2, float64())
    } else if (.Generic == "%/%") {
      # In R, integer division works like floor(float division)
      out <- build_array_expression("/", e1, e2)
      return(cast_array_expression(out, int32(), allow_float_truncate = TRUE))
    } else if (.Generic == "%%") {
      # {e1 - e2 * ( e1 %/% e2 )}
      # ^^^ form doesn't work because Ops.Array evaluates eagerly,
      # but we can build that up
      quotient <- build_array_expression("%/%", e1, e2)
      # this cast is to ensure that the result of this and e1 are the same
      # (autocasting only applies to scalars)
      base <- cast_array_expression(quotient * e2, e1$type)
      return(build_array_expression("-", e1, base))
    }

    expr <- array_expression(.binary_function_map[[.Generic]], e1, e2, ...)
  }
  expr
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
  "is.na" = "is_null"
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
  # TODO: "^"  (ARROW-11070)
  "%in%" = "is_in_meta_binary"
)

.array_function_map <- c(.unary_function_map, .binary_function_map)

eval_array_expression <- function(x) {
  x$args <- lapply(x$args, function (a) {
    if (inherits(a, "array_expression")) {
      eval_array_expression(a)
    } else {
      a
    }
  })
  if (length(x$args) == 2L) {
    # Insert implicit casts
    if (inherits(x$args[[1]], "Scalar")) {
      x$args[[1]] <- x$args[[1]]$cast(x$args[[2]]$type)
    } else if (inherits(x$args[[2]], "Scalar")) {
      x$args[[2]] <- x$args[[2]]$cast(x$args[[1]]$type)
    } else if (x$fun == "is_in_meta_binary" && inherits(x$args[[2]], "Array")) {
      x$args[[2]] <- x$args[[2]]$cast(x$args[[1]]$type)
    }
  }
  call_function(x$fun, args = x$args, options = x$options %||% empty_named_list())
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
  # Prune this for readability
  function_name <- sub("_kleene", "", x$fun)
  paste0(function_name, "(", paste(printed_args, collapse = ", "), ")")
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

build_dataset_expression <- function(.Generic, e1, e2, ...) {
  if (.Generic %in% names(.unary_function_map)) {
    expr <- Expression$create(.unary_function_map[[.Generic]], e1)
  } else if (.Generic == "%in%") {
    # Special-case %in%, which is different from the Array function name
    expr <- Expression$create("is_in", e1,
      options = list(
        value_set = Array$create(e2),
        skip_nulls = TRUE
      )
    )
  } else {
    if (!inherits(e1, "Expression")) {
      e1 <- Expression$scalar(e1)
    }
    if (!inherits(e2, "Expression")) {
      e2 <- Expression$scalar(e2)
    }

    # In Arrow, "divide" is one function, which does integer division on
    # integer inputs and floating-point division on floats
    if (.Generic == "/") {
      # TODO: omg so many ways it's wrong to assume these types
      e1 <- e1$cast(float64())
      e2 <- e2$cast(float64())
    } else if (.Generic == "%/%") {
      # In R, integer division works like floor(float division)
      out <- build_dataset_expression("/", e1, e2)
      return(out$cast(int32(), allow_float_truncate = TRUE))
    } else if (.Generic == "%%") {
      return(e1 - e2 * ( e1 %/% e2 ))
    }

    expr <- Expression$create(.binary_function_map[[.Generic]], e1, e2, ...)
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
