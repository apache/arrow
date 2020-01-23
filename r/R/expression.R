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
is.na.Array <- function(x) array_expression("is.na", x)

#' @export
is.na.ChunkedArray <- is.na.Array

#' @export
is.na.array_expression <- is.na.Array

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
#' [Scanner]. `FieldExpression`s refer to columns in the `Dataset` and are
#' compared to `ScalarExpression`s using `ComparisonExpression`s.
#' `ComparisonExpression`s may be combined with `AndExpression` or
#' `OrExpression` and negated with `NotExpression`. `IsValidExpression` is
#' essentially `is.na()` for `Expression`s.
#'
#' @section Factory:
#' `FieldExpression$create(name)` takes a string name as input. This string should
#' refer to a column in a `Dataset` at the time it is evaluated, but you can
#' construct a `FieldExpression` independently of any `Dataset`.
#'
#' `ScalarExpression$create(x)` takes a scalar (length-1) R value as input.
#'
#' `ComparisonExpression$create(OP, e1, e2)` takes a string operator name
#' (e.g. "==", "!=", ">", etc.) and two `Expression` objects.
#'
#' `AndExpression$create(e1, e2)` and `OrExpression$create(e1, e2)` take
#' two `Expression` objects, while `NotExpression$create(e1)` and
#' `IsValidExpression$create(e1)` take a single `Expression`.
#' @name Expression
#' @rdname Expression
#' @export
Expression <- R6Class("Expression", inherit = Object,
  public = list(
    ToString = function() dataset___expr__ToString(self)
  )
)

#' @usage NULL
#' @format NULL
#' @rdname Expression
#' @export
FieldExpression <- R6Class("FieldExpression", inherit = Expression)
FieldExpression$create <- function(name) {
  assert_is(name, "character")
  assert_that(length(name) == 1)
  shared_ptr(FieldExpression, dataset___expr__field_ref(name))
}

#' @usage NULL
#' @format NULL
#' @rdname Expression
#' @export
ScalarExpression <- R6Class("ScalarExpression", inherit = Expression)
ScalarExpression$create <- function(x) {
  stopifnot(vec_size(x) == 1L || is.null(x))
  shared_ptr(ScalarExpression, dataset___expr__scalar(x))
}

#' @usage NULL
#' @format NULL
#' @rdname Expression
#' @export
ComparisonExpression <- R6Class("ComparisonExpression", inherit = Expression)
ComparisonExpression$create <- function(OP, e1, e2) {
  comp_func <- comparison_function_map[[OP]]
  if (is.null(comp_func)) {
    stop(OP, " is not a supported comparison function", call. = FALSE)
  }
  shared_ptr(ComparisonExpression, comp_func(e1, e2))
}

comparison_function_map <- list(
  "==" = dataset___expr__equal,
  "!=" = dataset___expr__not_equal,
  ">" = dataset___expr__greater,
  ">=" = dataset___expr__greater_equal,
  "<" = dataset___expr__less,
  "<=" = dataset___expr__less_equal
)

#' @usage NULL
#' @format NULL
#' @rdname Expression
#' @export
InExpression <- R6Class("InExpression", inherit = Expression)
InExpression$create <- function(x, table) {
  shared_ptr(InExpression, dataset___expr__in(x, Array$create(table)))
}

#' @usage NULL
#' @format NULL
#' @rdname Expression
#' @export
AndExpression <- R6Class("AndExpression", inherit = Expression)
AndExpression$create <- function(e1, e2) {
  shared_ptr(AndExpression, dataset___expr__and(e1, e2))
}
#' @usage NULL
#' @format NULL
#' @rdname Expression
#' @export
OrExpression <- R6Class("OrExpression", inherit = Expression)
OrExpression$create <- function(e1, e2) {
  shared_ptr(OrExpression, dataset___expr__or(e1, e2))
}
#' @usage NULL
#' @format NULL
#' @rdname Expression
#' @export
NotExpression <- R6Class("NotExpression", inherit = Expression)
NotExpression$create <- function(e1) {
  shared_ptr(NotExpression, dataset___expr__not(e1))
}

#' @usage NULL
#' @format NULL
#' @rdname Expression
#' @export
IsValidExpression <- R6Class("IsValidExpression", inherit = Expression)
IsValidExpression$create <- function(e1) {
  shared_ptr(IsValidExpression, dataset___expr__is_valid(e1))
}

#' @export
Ops.Expression <- function(e1, e2) {
  if (.Generic == "!") {
    return(NotExpression$create(e1))
  }
  make_expression(.Generic, e1, e2)
}

make_expression <- function(operator, e1, e2) {
  if (operator == "%in%") {
    # In doesn't take Scalar, it takes Array
    return(InExpression$create(e1, e2))
  }
  # Check for non-expressions and convert to ScalarExpressions
  if (!inherits(e1, "Expression")) {
    e1 <- ScalarExpression$create(e1)
  }
  if (!inherits(e2, "Expression")) {
    e2 <- ScalarExpression$create(e2)
  }
  if (operator == "&") {
    AndExpression$create(e1, e2)
  } else if (operator == "|") {
    OrExpression$create(e1, e2)
  } else {
    ComparisonExpression$create(operator, e1, e2)
  }
}

#' @export
is.na.Expression <- function(x) !IsValidExpression$create(x)
