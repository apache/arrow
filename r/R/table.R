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

#' @include arrow-package.R
#'
#' @title class arrow::Table
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Factory:
#'
#' The `Table$create()` function takes the following arguments:
#'
#' * `...` arrays, chunked arrays, or R vectors
#' * `schema` a schema. The default (`NULL`) infers the schema from the `...`
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname Table
#' @name Table
#' @export
Table <- R6Class("Table", inherit = Object,
  public = list(
    column = function(i) shared_ptr(ChunkedArray, Table__column(self, i)),
    field = function(i) shared_ptr(Field, Table__field(self, i)),

    serialize = function(output_stream, ...) write_table(self, output_stream, ...),

    cast = function(target_schema, safe = TRUE, options = cast_options(safe)) {
      assert_is(target_schema, "Schema")
      assert_is(options, "CastOptions")
      assert_that(identical(self$schema$names, target_schema$names), msg = "incompatible schemas")
      shared_ptr(Table, Table__cast(self, target_schema, options))
    },

    select = function(spec) {
      spec <- enquo(spec)
      if (quo_is_null(spec)) {
        self
      } else {
        all_vars <- Table__column_names(self)
        vars <- vars_select(all_vars, !!spec)
        indices <- match(vars, all_vars)
        shared_ptr(Table, Table__select(self, indices))
      }

    }
  ),

  active = list(
    num_columns = function() Table__num_columns(self),
    num_rows = function() Table__num_rows(self),
    schema = function() shared_ptr(Schema, Table__schema(self)),
    columns = function() map(Table__columns(self), shared_ptr, class = Column)
  )
)

Table$create <- function(..., schema = NULL){
  dots <- list2(...)
  # making sure there are always names
  if (is.null(names(dots))) {
    names(dots) <- rep_len("", length(dots))
  }
  stopifnot(length(dots) > 0)
  shared_ptr(Table, Table__from_dots(dots, schema))
}

#' @export
as.data.frame.Table <- function(x, row.names = NULL, optional = FALSE, use_threads = TRUE, ...){
  Table__to_dataframe(x, use_threads = option_use_threads())
}

#' @export
dim.Table <- function(x) {
  c(x$num_rows, x$num_columns)
}
