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

`arrow::RecordBatch` <- R6Class("arrow::RecordBatch", inherit = `arrow::Object`,
  public = list(
    num_columns = function() RecordBatch__num_columns(self),
    num_rows = function() RecordBatch__num_rows(self),
    schema = function() `arrow::Schema`$new(RecordBatch__schema(self)),
    to_file = function(path) invisible(RecordBatch__to_file(self, fs::path_abs(path))),
    to_stream = function() RecordBatch__to_stream(self),
    column = function(i) `arrow::Array`$new(RecordBatch__column(self, i)),
    column_name = function(i) RecordBatch__column_name(self, i),
    names = function() RecordBatch__names(self),
    Equals = function(other) {
      assert_that(inherits(other, "arrow::RecordBatch"))
      RecordBatch__Equals(self, other)
    },
    RemoveColumn = function(i){
      `arrow::RecordBatch`$new(RecordBatch__RemoveColumn(self, i))
    },
    Slice = function(offset, length = NULL) {
      if (is.null(length)) {
        `arrow::RecordBatch`$new(RecordBatch__Slice1(self, offset))
      } else {
        `arrow::RecordBatch`$new(RecordBatch__Slice2(self, offset, length))
      }
    }
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
`as_tibble.arrow::RecordBatch` <- function(x, ...){
  RecordBatch__to_dataframe(x)
}

#' Create an arrow::RecordBatch from a data frame
#'
#' @param .data a data frame
#'
#' @export
record_batch <- function(.data){
  `arrow::RecordBatch`$new(RecordBatch__from_dataframe(.data))
}

#' Read a single record batch from a stream
#'
#' @param stream input stream
#'
#' @details `stream` can be a `arrow::io::RandomAccessFile` stream as created by [file_open()] or [mmap_open()] or a path.
#'
#' @export
read_record_batch <- function(stream){
  UseMethod("read_record_batch")
}

#' @export
read_record_batch.character <- function(stream){
  assert_that(length(stream) == 1L)
  read_record_batch(fs::path_abs(stream))
}

#' @export
read_record_batch.fs_path <- function(stream){
  stream <- file_open(stream); on.exit(stream$Close())
  read_record_batch(stream)
}

#' @export
`read_record_batch.arrow::io::RandomAccessFile` <- function(stream){
  `arrow::RecordBatch`$new(read_record_batch_RandomAccessFile(stream))
}

#' @export
`read_record_batch.arrow::io::BufferReader` <- function(stream){
  `arrow::RecordBatch`$new(read_record_batch_BufferReader(stream))
}

#' @export
read_record_batch.raw <- function(stream){
  stream <- buffer_reader(stream); on.exit(stream$Close())
  read_record_batch(stream)
}

