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

array_expression <- function(FUN, ...) {
  structure(list(fun = FUN, args = list(...)), class = "array_expression")
}

#' @export
Ops.Array <- function(e1, e2) array_expression(.Generic, e1, e2)

#' @export
Ops.ChunkedArray <- Ops.Array

#' @export
Ops.array_expression <- Ops.Array

#' @export
is.na.array_expression <- function(x) array_expression("is.na", x)

#' @export
as.vector.array_expression <- function(x, ...) {
  x$args <- lapply(x$args, as.vector)
  do.call(x$fun, x$args)
}

#' @export
print.array_expression <- function(x, ...) print(as.vector(x))

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
#' `Expression$compare(OP, e1, e2)` takes two `Expression` operands, constructing
#' an `Expression` which will evaluate these operands then compare them with the
#' relation specified by OP (e.g. "==", "!=", ">", etc.) For example, to filter
#' down to rows where the column named "alpha" is less than 5:
#' `Expression$compare("<", Expression$field_ref("alpha"), Expression$scalar(5))`
#'
#' `Expression$and(e1, e2)`, `Expression$or(e1, e2)`, and `Expression$not(e1)`
#' construct an `Expression` combining their arguments with Boolean operators.
#'
#' `Expression$is_valid(x)` is essentially (an inversion of) `is.na()` for `Expression`s.
#'
#' `Expression$in_(x, set)` evaluates x and returns whether or not it is a member of the set.
#' @name Expression
#' @rdname Expression
#' @export
Expression <- R6Class("Expression", inherit = ArrowObject,
  public = list(
    ToString = function() dataset___expr__ToString(self)
  )
)

Expression$field_ref <- function(name) {
  assert_is(name, "character")
  assert_that(length(name) == 1)
  shared_ptr(Expression, dataset___expr__field_ref(name))
}
Expression$scalar <- function(x) {
  shared_ptr(Expression, dataset___expr__scalar(Scalar$create(x)))
}
Expression$compare <- function(OP, e1, e2) {
  comp_func <- comparison_function_map[[OP]]
  if (is.null(comp_func)) {
    stop(OP, " is not a supported comparison function", call. = FALSE)
  }
  shared_ptr(Expression, comp_func(e1, e2))
}

comparison_function_map <- list(
  "==" = dataset___expr__equal,
  "!=" = dataset___expr__not_equal,
  ">" = dataset___expr__greater,
  ">=" = dataset___expr__greater_equal,
  "<" = dataset___expr__less,
  "<=" = dataset___expr__less_equal
)
Expression$in_ <- function(x, set) {
  shared_ptr(Expression, dataset___expr__in(x, Array$create(set)))
}
Expression$and <- function(e1, e2) {
  shared_ptr(Expression, dataset___expr__and(e1, e2))
}
Expression$or <- function(e1, e2) {
  shared_ptr(Expression, dataset___expr__or(e1, e2))
}
Expression$not <- function(e1) {
  shared_ptr(Expression, dataset___expr__not(e1))
}
Expression$is_valid <- function(e1) {
  shared_ptr(Expression, dataset___expr__is_valid(e1))
}

#' @export
Ops.Expression <- function(e1, e2) {
  if (.Generic == "!") {
    return(Expression$not(e1))
  }
  make_expression(.Generic, e1, e2)
}

make_expression <- function(operator, e1, e2) {
  if (operator == "%in%") {
    # In doesn't take Scalar, it takes Array
    return(Expression$in_(e1, e2))
  }
  # Check for non-expressions and convert to Expressions
  if (!inherits(e1, "Expression")) {
    e1 <- Expression$scalar(e1)
  }
  if (!inherits(e2, "Expression")) {
    e2 <- Expression$scalar(e2)
  }
  if (operator == "&") {
    Expression$and(e1, e2)
  } else if (operator == "|") {
    Expression$or(e1, e2)
  } else {
    Expression$compare(operator, e1, e2)
  }
}

#' @export
is.na.Expression <- function(x) !Expression$is_valid(x)
