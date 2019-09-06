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
#' - [arrow::RecordBatchStreamWriter][RecordBatchStreamWriter] implements the streaming binary format
#' - [arrow::RecordBatchFileWriter][RecordBatchFileWriter] implements the binary file format
#'
#' @rdname RecordBatchWriter
#' @name RecordBatchWriter
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
#' writer <- RecordBatchStreamWriter$create(sink, schema)
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
#' inherited from [arrow::RecordBatchWriter][RecordBatchWriter]
#'
#' - `$write_batch(batch)`: Write record batch to stream
#' - `$write_table(table)`: write Table to stream
#' - `$close()`: close stream
#'
#' @rdname RecordBatchStreamWriter
#' @name RecordBatchStreamWriter
RecordBatchStreamWriter <- R6Class("RecordBatchStreamWriter", inherit = RecordBatchWriter)

RecordBatchStreamWriter$create <- function(sink, schema) {
  if (is.character(sink)) {
    sink <- FileOutputStream$create(sink)
  }
  assert_that(inherits(sink, "OutputStream"))
  assert_that(inherits(schema, "Schema"))

  shared_ptr(RecordBatchStreamWriter, ipc___RecordBatchStreamWriter__Open(sink, schema))
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
#' writer <- RecordBatchFileWriter$create(sink, schema)
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
#' inherited from [arrow::RecordBatchWriter][RecordBatchWriter]
#'
#' - `$write_batch(batch)`: Write record batch to stream
#' - `$write_table(table)`: write Table to stream
#' - `$close()`: close stream
#'
#' @rdname RecordBatchFileWriter
#' @name RecordBatchFileWriter
RecordBatchFileWriter <- R6Class("RecordBatchFileWriter", inherit = RecordBatchStreamWriter)

RecordBatchFileWriter$create <- function(sink, schema) {
  if (is.character(sink)) {
    sink <- FileOutputStream$create(sink)
  }
  assert_that(inherits(sink, "OutputStream"))
  assert_that(inherits(schema, "Schema"))

  shared_ptr(RecordBatchFileWriter, ipc___RecordBatchFileWriter__Open(sink, schema))
}
