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

#' @title class arrow::RecordBatchWriter
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' - `$write_batch(batch)`: Write record batch to stream
#' - `$write_table(table)`: write Table to stream
#' - `$close()`: close stream
#'
#' @section Derived classes:
#'
#' - [arrow::RecordBatchStreamWriter][arrow__ipc__RecordBatchStreamWriter] implements the streaming binary format
#' - [arrow::RecordBatchFileWriter][arrow__ipc__RecordBatchFileWriter] implements the binary file format
#'
#' @rdname arrow__ipc__RecordBatchWriter
#' @name arrow__ipc__RecordBatchWriter
`arrow::RecordBatchWriter` <- R6Class("arrow::RecordBatchWriter", inherit = Object,
  public = list(
    write_batch = function(batch) ipc___RecordBatchWriter__WriteRecordBatch(self, batch),
    write_table = function(table) ipc___RecordBatchWriter__WriteTable(self, table),

    write = function(x) {
      if (inherits(x, "arrow::RecordBatch")) {
        self$write_batch(x)
      } else if (inherits(x, "arrow::Table")) {
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

#' @title class arrow::RecordBatchStreamWriter
#'
#' Writer for the Arrow streaming binary format
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section usage:
#'
#' ```
#' writer <- RecordBatchStreamWriter(sink, schema)
#'
#' writer$write_batch(batch)
#' writer$write_table(table)
#' writer$close()
#' ```
#'
#' @section Factory:
#'
#' The [RecordBatchStreamWriter()] function creates a record batch stream writer.
#'
#' @section Methods:
#' inherited from [arrow::RecordBatchWriter][arrow__ipc__RecordBatchWriter]
#'
#' - `$write_batch(batch)`: Write record batch to stream
#' - `$write_table(table)`: write Table to stream
#' - `$close()`: close stream
#'
#' @rdname arrow__ipc__RecordBatchStreamWriter
#' @name arrow__ipc__RecordBatchStreamWriter
`arrow::RecordBatchStreamWriter` <- R6Class("arrow::RecordBatchStreamWriter", inherit = `arrow::RecordBatchWriter`)

#' Writer for the Arrow streaming binary format
#'
#' @param sink Where to write. Can either be:
#'
#' - A string file path
#' - [arrow::io::OutputStream][arrow__io__OutputStream]
#'
#' @param schema The [arrow::Schema][arrow__Schema] for data to be written.
#'
#' @return a [arrow::RecordBatchStreamWriter][arrow__ipc__RecordBatchStreamWriter]
#'
#' @export
RecordBatchStreamWriter <- function(sink, schema) {
  UseMethod("RecordBatchStreamWriter")
}

#' @export
RecordBatchStreamWriter.character <- function(sink, schema){
  RecordBatchStreamWriter(FileOutputStream$create(sink), schema)
}

#' @export
RecordBatchStreamWriter.OutputStream <- function(sink, schema){
  assert_that(inherits(schema, "arrow::Schema"))
  shared_ptr(`arrow::RecordBatchStreamWriter`, ipc___RecordBatchStreamWriter__Open(sink, schema))
}

#' @title class arrow::RecordBatchFileWriter
#'
#' Writer for the Arrow binary file format
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section usage:
#'
#' ```
#' writer <- RecordBatchFileWriter(sink, schema)
#'
#' writer$write_batch(batch)
#' writer$write_table(table)
#' writer$close()
#' ```
#'
#' @section Factory:
#'
#' The [RecordBatchFileWriter()] function creates a record batch stream writer.
#'
#' @section Methods:
#' inherited from [arrow::RecordBatchWriter][arrow__ipc__RecordBatchWriter]
#'
#' - `$write_batch(batch)`: Write record batch to stream
#' - `$write_table(table)`: write Table to stream
#' - `$close()`: close stream
#'
#' @rdname arrow__ipc__RecordBatchFileWriter
#' @name arrow__ipc__RecordBatchFileWriter
`arrow::RecordBatchFileWriter` <- R6Class("arrow::RecordBatchFileWriter", inherit = `arrow::RecordBatchStreamWriter`)

#' Create a record batch file writer from a stream
#'
#' @param sink Where to write. Can either be:
#'
#' - a string file path
#' - [arrow::io::OutputStream][arrow__io__OutputStream]
#'
#' @param schema The [arrow::Schema][arrow__Schema] for data to be written.
#'
#' @return an `arrow::RecordBatchWriter` object
#'
#' @export
RecordBatchFileWriter <- function(sink, schema) {
  UseMethod("RecordBatchFileWriter")
}

#' @export
RecordBatchFileWriter.character <- function(sink, schema){
  RecordBatchFileWriter(FileOutputStream$create(sink), schema)
}

#' @export
RecordBatchFileWriter.OutputStream <- function(sink, schema){
  assert_that(inherits(schema, "arrow::Schema"))
  shared_ptr(`arrow::RecordBatchFileWriter`, ipc___RecordBatchFileWriter__Open(sink, schema))
}
