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

#' @title class arrow::RecordBatch
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname arrow__RecordBatch
#' @name arrow__RecordBatch
`arrow::RecordBatch` <- R6Class("arrow::RecordBatch", inherit = `arrow::Object`,
  public = list(
    column = function(i) shared_ptr(`arrow::Array`, RecordBatch__column(self, i)),
    column_name = function(i) RecordBatch__column_name(self, i),
    names = function() RecordBatch__names(self),
    Equals = function(other) {
      assert_that(inherits(other, "arrow::RecordBatch"))
      RecordBatch__Equals(self, other)
    },

    RemoveColumn = function(i){
      shared_ptr(`arrow::RecordBatch`, RecordBatch__RemoveColumn(self, i))
    },

    Slice = function(offset, length = NULL) {
      if (is.null(length)) {
        shared_ptr(`arrow::RecordBatch`, RecordBatch__Slice1(self, offset))
      } else {
        shared_ptr(`arrow::RecordBatch`, RecordBatch__Slice2(self, offset, length))
      }
    },

    serialize = function() ipc___SerializeRecordBatch__Raw(self),

    cast = function(target_schema, safe = TRUE, options = cast_options(safe)) {
      assert_that(inherits(target_schema, "arrow::Schema"))
      assert_that(inherits(options, "arrow::compute::CastOptions"))
      assert_that(identical(self$schema$names, target_schema$names), msg = "incompatible schemas")
      shared_ptr(`arrow::RecordBatch`, RecordBatch__cast(self, target_schema, options))
    }
  ),

  active = list(
    num_columns = function() RecordBatch__num_columns(self),
    num_rows = function() RecordBatch__num_rows(self),
    schema = function() shared_ptr(`arrow::Schema`, RecordBatch__schema(self)),
    columns = function() map(RecordBatch__columns(self), shared_ptr, `arrow::Array`)
  )
)

#' @export
`names.arrow::RecordBatch` <- function(x) {
  x$names()
}

#' @export
`==.arrow::RecordBatch` <- function(x, y) {
  x$Equals(y)
}

#' @export
`dim.arrow::RecordBatch` <- function(x) {
  c(x$num_rows, x$num_columns)
}

#' @export
`as.data.frame.arrow::RecordBatch` <- function(x, row.names = NULL, optional = FALSE, use_threads = TRUE, ...){
  RecordBatch__to_dataframe(x, use_threads = option_use_threads())
}

#' Create an [arrow::RecordBatch][arrow__RecordBatch] from a data frame
#'
#' @param ... A variable number of arrow::Array
#' @param schema a arrow::Schema
#'
#' @return a [arrow::RecordBatch][arrow__RecordBatch]
#' @export
record_batch <- function(..., schema = NULL){
  arrays <- list2(...)
  stopifnot(length(arrays) > 0)
  shared_ptr(`arrow::RecordBatch`, RecordBatch__from_arrays(schema, arrays))
}
