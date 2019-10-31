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

#' @export
Ops.Array <- function(e1, e2) {
  structure(list(fun = .Generic, args = list(e1, e2)), class = "array_expression")
}

#' @export
Ops.ChunkedArray <- Ops.Array

#' @export
Ops.array_expression <- Ops.Array

#' @export
as.vector.array_expression <- function(x, ...) {
  x$args <- lapply(x$args, as.vector)
  do.call(x$fun, x$args)
}

#' @export
print.array_expression <- function(x, ...) print(as.vector(x))

###########

#' @export
Expression <- R6Class("Expression", inherit = Object,
  public = list()
)

#' @export
FieldExpression <- R6Class("FieldExpression", inherit = Expression,
  public = list()
)
FieldExpression$create <- function(name) {
  assert_is(name, "character")
  assert_that(length(name) == 1)
  shared_ptr(FieldExpression, dataset___expr__field_ref(name))
}

#' @export
ScalarExpression <- R6Class("ScalarExpression", inherit = Expression,
  public = list()
)
ScalarExpression$create <- function(x) {
  shared_ptr(ScalarExpression, dataset___expr__scalar(x))
}

#' @export
CompareExpression <- R6Class("CompareExpression", inherit = Expression,
  public = list()
)
CompareExpression$create <- function(FUN, e1, e2) {
  # TODO: map FUN to functions, not just equal
  shared_ptr(CompareExpression, dataset___expr__equal(e1, e2))
}

Ops.Expression <- function(e1, e2) {
  # Check for non-expressions and convert to ScalarExpressions
  if (!inherits(e1, "Expression")) {
    e1 <- ScalarExpression$create(e1)
  }
  if (!inherits(e2, "Expression")) {
    e2 <- ScalarExpression$create(e2)
  }
  CompareExpression$create(.Generic, e1, e2)
}
