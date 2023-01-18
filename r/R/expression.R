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
#' `Expression` containing one or more `Expression`s. Anything in `...` that
#' is not already an expression will be wrapped in `Expression$scalar()`.
#'
#' `Expression$op(FUN, ...)` is for logical and arithmetic operators. Scalar
#' inputs in `...` will be attempted to be cast to the common type of the
#' `Expression`s in the call so that the types of the columns in the `Dataset`
#' are preserved and not unnecessarily upcast, which may be expensive.
#' @name Expression
#' @rdname Expression
#' @include arrowExports.R
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
    is_field_ref = function() {
      compute___expr__is_field_ref(self)
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
  # Make sure all inputs are Expressions
  args <- lapply(args, function(x) {
    if (!inherits(x, "Expression")) {
      x <- Expression$scalar(x)
    }
    x
  })
  expr <- compute___expr__call(function_name, args, options)
  if (length(args)) {
    expr$schema <- unify_schemas(schemas = lapply(args, function(x) x$schema))
  } else {
    # TODO: this shouldn't be necessary
    expr$schema <- schema()
  }
  expr
}


#' @export
`[[.Expression` <- function(x, i, ...) get_nested_field(x, i)

#' @export
`$.Expression` <- function(x, name, ...) {
  assert_that(is.string(name))
  if (name %in% ls(x)) {
    get(name, x)
  } else {
    get_nested_field(x, name)
  }
}

get_nested_field <- function(expr, name) {
  if (expr$is_field_ref()) {
    # Make a nested field ref
    # TODO(#33756): integer (positional) field refs are supported in C++
    assert_that(is.string(name))
    out <- compute___expr__nested_field_ref(expr, name)
  } else {
    # Use the struct_field kernel if expr is a struct:
    expr_type <- expr$type() # errors if no schema set
    if (inherits(expr_type, "StructType")) {
      # Because we have the type, we can validate that the field exists
      if (!(name %in% names(expr_type))) {
        stop(
          "field '", name, "' not found in ",
          expr_type$ToString(),
          call. = FALSE
        )
      }
      out <- Expression$create(
        "struct_field",
        expr,
        options = list(field_ref = Expression$field_ref(name))
      )
    } else {
      # TODO(#33757): if expr is list type and name is integer or Expression,
      # call list_element
      stop(
        "Cannot extract a field from an Expression of type ", expr_type$ToString(),
        call. = FALSE
      )
    }
  }
  # Schema bookkeeping
  out$schema <- expr$schema
  out
}

Expression$field_ref <- function(name) {
  # TODO(#33756): allow construction of field ref from integer
  assert_that(is.string(name))
  compute___expr__field_ref(name)
}
Expression$scalar <- function(x) {
  if (!inherits(x, "Scalar")) {
    x <- Scalar$create(x)
  }
  expr <- compute___expr__scalar(x)
  expr$schema <- schema()
  expr
}
# Wrapper around Expression$create that:
# (1) maps R operator names to Arrow C++ compute ("/" --> "divide_checked").
#     This is convenient for Ops.Expression, despite the special handling
#     for the division operators inside the function
# (2) wraps R input args as Array or Scalar and attempts to cast them to
#     match the type of the columns/fields in the expression. This is to prevent
#     upcasting all of the data where a simple downcast of a Scalar works.
Expression$op <- function(FUN,
                          ...,
                          args = list(...)) {
  if (FUN == "-" && length(args) == 1L) {
    if (inherits(args[[1]], c("ArrowObject", "Expression"))) {
      return(Expression$create("negate_checked", args[[1]]))
    } else {
      return(-args[[1]])
    }
  }

  if (FUN != "%/%") {
    # We switch %/% behavior based on the actual input types so don't
    # try to cast scalars to match the columns
    args <- cast_scalars_to_common_type(args)
  }

  # In Arrow, "divide" is one function, which does integer division on
  # integer inputs and floating-point division on floats
  if (FUN == "/") {
    # TODO: omg so many ways it's wrong to assume these types (right?)
    args <- lapply(args, cast, float64())
  } else if (FUN == "%/%") {
    # In R, integer division works like floor(float division)
    out <- Expression$create("floor", Expression$op("/", args = args))

    # ... but if inputs are integer, make sure we return an integer
    int_type_ids <- Type[toupper(INTEGER_TYPES)]
    is_int <- function(x) {
      is.integer(x) ||
        (inherits(x, "ArrowObject") && x$type_id() %in% int_type_ids)
    }

    if (is_int(args[[1]]) && is_int(args[[2]])) {
      if (inherits(args[[1]], "ArrowObject")) {
        out_type <- args[[1]]$type()
      } else {
        # It's an R integer
        out_type <- int32()
      }
      # If args[[2]] == 0, float division returns Inf,
      # but for integer division R returns NA, so wrap in if_else
      out <- Expression$create(
        "if_else",
        Expression$op("==", args[[2]], 0L),
        Scalar$create(NA_integer_, out_type),
        cast(out, out_type, allow_float_truncate = TRUE)
      )
    }
    return(out)
  } else if (FUN == "%%") {
    return(args[[1]] - args[[2]] * (args[[1]] %/% args[[2]]))
  }

  Expression$create(.operator_map[[FUN]], args = args)
}

#' @export
Ops.Expression <- function(e1, e2) {
  if (.Generic == "!") {
    Expression$create("invert", e1)
  } else {
    Expression$op(.Generic, e1, e2)
  }
}

#' @export
is.na.Expression <- function(x) {
  Expression$create("is_null", x, options = list(nan_is_null = TRUE))
}
