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
#' `Expression` containing one or more `Expression`s.
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

Expression$field_ref <- function(name) {
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

#' @export
Ops.Expression <- function(e1, e2) {
  if (.Generic == "!") {
    Expression$create("invert", e1)
  } else {
    build_expr(.Generic, e1, e2)
  }
}

#' @export
is.na.Expression <- function(x) {
  Expression$create("is_null", x, options = list(nan_is_null = TRUE))
}
