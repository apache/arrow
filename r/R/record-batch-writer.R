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

#' @title RecordBatchWriter classes
#' @description `RecordBatchFileWriter` and `RecordBatchStreamWriter` are
#' interfaces for writing record batches to either the binary file or streaming
#' format.
#' @usage NULL
#' @format NULL
#' @docType class
#' @section Usage:
#'
#' ```
#' writer <- RecordBatchStreamWriter$create(sink, schema)
#'
#' writer$write_batch(batch)
#' writer$write_table(table)
#' writer$close()
#' ```
#' @section Factory:
#'
#' The `RecordBatchFileWriter$create()` and `RecordBatchStreamWriter$create()`
#' factory methods instantiate the object and
#' take a single argument, named according to the class:
#'
#' - `sink` A character file name or an `OutputStream`.
#' - `schema` A [Schema] for the data to be written.
#'
#' @section Methods:
#'
#' - `$write(x)`: Write a [RecordBatch], [Table], or `data.frame`, dispatching
#'    to the methods below appropriately
#' - `$write_batch(batch)`: Write a `RecordBatch` to stream
#' - `$write_table(table)`: Write a `Table` to stream
#' - `$close()`: close stream
#'
#' @rdname RecordBatchWriter
#' @name RecordBatchWriter
#' @include arrow-package.R
RecordBatchWriter <- R6Class("RecordBatchWriter", inherit = Object,
  public = list(
    write_batch = function(batch) ipc___RecordBatchWriter__WriteRecordBatch(self, batch),
    write_table = function(table) ipc___RecordBatchWriter__WriteTable(self, table),

    write = function(x) {
      if (inherits(x, "RecordBatch")) {
        self$write_batch(x)
      } else if (inherits(x, "Table")) {
        self$write_table(x)
      } else if (inherits(x, "data.frame")) {
        self$write_table(table(x))
      } else {
        abort("unexpected type for RecordBatchWriter$write(), must be an arrow::RecordBatch or an arrow::Table")
      }
    },

    close = function() ipc___RecordBatchWriter__Close(self)
  )
)

#' @usage NULL
#' @format NULL
#' @rdname RecordBatchWriter
#' @export
RecordBatchStreamWriter <- R6Class("RecordBatchStreamWriter", inherit = RecordBatchWriter)
RecordBatchStreamWriter$create <- function(sink, schema) {
  if (is.character(sink)) {
    sink <- FileOutputStream$create(sink)
  }
  assert_is(sink, "OutputStream")
  assert_is(schema, "Schema")

  shared_ptr(RecordBatchStreamWriter, ipc___RecordBatchStreamWriter__Open(sink, schema))
}

#' @usage NULL
#' @format NULL
#' @rdname RecordBatchWriter
#' @export
RecordBatchFileWriter <- R6Class("RecordBatchFileWriter", inherit = RecordBatchStreamWriter)
RecordBatchFileWriter$create <- function(sink, schema) {
  if (is.character(sink)) {
    sink <- FileOutputStream$create(sink)
  }
  assert_is(sink, "OutputStream")
  assert_is(schema, "Schema")

  shared_ptr(RecordBatchFileWriter, ipc___RecordBatchFileWriter__Open(sink, schema))
}
