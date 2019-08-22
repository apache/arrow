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

#' Send R data structures to Arrow
#'
#' @param x an R vector or `data.frame`
#' @param ... additional arguments passed to some methods:
#' * `table` logical: when providing a `data.frame` input, should it be made
#' into an Arrow Table or a struct-type Array? Default is `TRUE` unless you
#' specify a `type`.
#' * `type` an explicit [type][arrow__DataType], or NULL (the default) to
#' infer from `x`. Only valid when making an `Array`.
#' * `schema` a schema. The default (`NULL`) infers the schema from the `x`.
#' Only valid when making a `Table` from a `data.frame`
#' @return An `arrow::Table` if `x` is a `data.frame` unless otherwise directed,
#' or an `arrow::Array`.
#' @examples
#' \donttest{
#' tbl <- data.frame(
#'   int = 1:10,
#'   dbl = as.numeric(1:10),
#'   lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
#'   chr = letters[1:10],
#'   stringsAsFactors = FALSE
#' )
#' tab <- to_arrow(tbl)
#' tab$schema
#'
#' a <- to_arrow(tbl$int)
#'
#' # Making a struct column from a data.frame
#' df <- tibble::tibble(x = 1:10, y = 1:10)
#' a <- to_arrow(df, table = FALSE)
#' # Or specify a type
#' a <- to_arrow(df, type = struct(x = float64(), y = int16()))
#' }
#' @export
to_arrow <- function(x, ...) {
  UseMethod("to_arrow")
}

#' @export
`to_arrow.arrow::Object` <- function(x, ...) x

#' @export
to_arrow.data.frame <- function(x, table = is.null(type), type = NULL, schema = NULL, ...) {
  # Validate that at least one of type or schema is null?
  if (table) {
    # Default: make an arrow Table
    shared_ptr(`arrow::Table`, Table__from_dots(x, schema_sxp = schema))
  } else {
    # Make this a struct array
    to_arrow.default(x, type = type)
  }
}

#' @export
to_arrow.default <- function(x, type = NULL, ...) {
  `arrow::Array`$dispatch(Array__from_vector(x, s_type = type))
}

#' Create an arrow::Table from diverse inputs
#'
#' Unlike [to_arrow()], this function splices together inputs to form a Table.
#' When providing columns, they can be a mix of Arrow arrays and R vectors.
#'
#' @param ... arrays, chunked arrays, or R vectors that should define the
#' columns of the Arrow Table; alternatively, if record batches are given,
#' they will be stacked.
#' @param schema a schema. The default (`NULL`) infers the schema from the `...`
#'
#' @return An `arrow::Table`
#' @examples
#' \donttest{
#' tab1 <- table_from_dots(a = 1:10, b = letters[1:10])
#' tab1
#' as.data.frame(tab1)
#' }
#' @export
table_from_dots <- function(..., schema = NULL){
  dots <- list2(...)
  # making sure there are always names
  if (is.null(names(dots))) {
    names(dots) <- rep_len("", length(dots))
  }
  stopifnot(length(dots) > 0)
  shared_ptr(`arrow::Table`, Table__from_dots(dots, schema))
}
