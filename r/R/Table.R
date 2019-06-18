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

#' @include R6.R
#'
#' @title class arrow::Table
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname arrow__Table
#' @name arrow__Table
`arrow::Table` <- R6Class("arrow::Table", inherit = `arrow::Object`,
  public = list(
    column = function(i) shared_ptr(`arrow::Column`, Table__column(self, i)),

    serialize = function(output_stream, ...) write_table(self, output_stream, ...),

    cast = function(target_schema, safe = TRUE, options = cast_options(safe)) {
      assert_that(inherits(target_schema, "arrow::Schema"))
      assert_that(inherits(options, "arrow::compute::CastOptions"))
      assert_that(identical(self$schema$names, target_schema$names), msg = "incompatible schemas")
      shared_ptr(`arrow::Table`, Table__cast(self, target_schema, options))
    }
  ),

  active = list(
    num_columns = function() Table__num_columns(self),
    num_rows = function() Table__num_rows(self),
    schema = function() shared_ptr(`arrow::Schema`, Table__schema(self)),
    columns = function() map(Table__columns(self), shared_ptr, class = `arrow::Column`)
  )
)

#' Create an arrow::Table from a data frame
#'
#' @param ... arrays, chunked arrays, or R vectors
#' @param schema a schema. The default (`NULL`) infers the schema from the `...`
#'
#' @return an arrow::Table
#'
#' @export
table <- function(..., schema = NULL){
  dots <- list2(...)
  stopifnot(length(dots) > 0)
  shared_ptr(`arrow::Table`, Table__from_dots(dots, schema))
}

#' @export
`as.data.frame.arrow::Table` <- function(x, row.names = NULL, optional = FALSE, use_threads = TRUE, ...){
  Table__to_dataframe(x, use_threads = option_use_threads())
}

#' @export
`dim.arrow::Table` <- function(x) {
  c(x$num_rows, x$num_columns)
}
