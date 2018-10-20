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

`arrow::ipc::RecordBatchWriter` <- R6Class("arrow::ipc::RecordBatchWriter", inherit = `arrow::Object`,
  public = list(
    WriteRecordBatch = function(batch, allow_64bit) ipc___RecordBatchWriter__WriteRecordBatch(self, batch, allow_64bit),
    WriteTable = function(table) ipc___RecordBatchWriter__WriteTable(self, table),
    Close = function() ipc___RecordBatchWriter__Close(self)
  )
)

`arrow::ipc::RecordBatchStreamWriter` <- R6Class("arrow::ipc::RecordBatchStreamWriter", inherit = `arrow::ipc::RecordBatchWriter`)
`arrow::ipc::RecordBatchFileWriter` <- R6Class("arrow::ipc::RecordBatchFileWriter", inherit = `arrow::ipc::RecordBatchStreamWriter`)

#' Create a record batch file writer from a stream
#'
#' @param stream a stream
#' @param schema the schema of the batches
#'
#' @return an `arrow::ipc::RecordBatchWriter` object
#'
#' @export
record_batch_file_writer <- function(stream, schema) {
  assert_that(
    inherits(stream, "arrow::io::OutputStream"),
    inherits(schema, "arrow::Schema")
  )
  construct(`arrow::ipc::RecordBatchFileWriter`, ipc___RecordBatchFileWriter__Open(stream, schema))
}

#' Create a record batch stream writer
#'
#' @param stream a stream
#' @param schema a schema
#'
#' @export
record_batch_stream_writer <- function(stream, schema) {
  assert_that(
    inherits(stream, "arrow::io::OutputStream"),
    inherits(schema, "arrow::Schema")
  )
  construct(`arrow::ipc::RecordBatchStreamWriter`, ipc___RecordBatchStreamWriter__Open(stream, schema))
}

#-------- write_record_batch

#' write a record batch
#'
#' @param x a `arrow::RecordBatch`
#' @param stream where to stream the record batch
#' @param ... extra parameters
#'
#' @export
write_record_batch <- function(x, stream, ...){
  UseMethod("write_record_batch", stream)
}

#' @export
`write_record_batch.arrow::io::OutputStream` <- function(x, stream, ...) {
  stream_writer <- close_on_exit(record_batch_stream_writer(stream, x$schema()))
  write_record_batch(x, stream_writer)
}

#' @export
`write_record_batch.arrow::ipc::RecordBatchWriter` <- function(x, stream, allow_64bit = TRUE, ...){
  stream$WriteRecordBatch(x, allow_64bit)
}

#' @export
`write_record_batch.character` <- function(x, stream, ...) {
  assert_that(length(stream) == 1L)
  write_record_batch(x, fs::path_abs(stream), ...)
}

#' @export
`write_record_batch.fs_path` <- function(x, stream, ...) {
  assert_that(length(stream) == 1L)
  file_stream <- close_on_exit(file_output_stream(stream))
  file_writer <- close_on_exit(record_batch_file_writer(file_stream, x$schema()))
  write_record_batch(x, file_writer, ...)
}

#' @export
`write_record_batch.raw` <- function(x, stream, ...) {
  # how many bytes do we need
  mock <- mock_output_stream()
  write_record_batch(x, mock)
  n <- mock$GetExtentBytesWritten()

  bytes <- raw(n)
  buffer <- buffer(bytes)
  buffer_writer <- fixed_size_buffer_writer(buffer)
  write_record_batch(x, buffer_writer)

  bytes
}

#-------- stream Table

#' write an arrow::Table
#'
#' @param x an `arrow::Table`
#' @param stream where to stream the record batch
#' @param ... extra parameters
#'
#' @export
write_table <- function(x, stream, ...) {
  UseMethod("write_table", stream)
}

#' @export
`write_table.arrow::io::OutputStream` <- function(x, stream, ...) {
  stream_writer <- close_on_exit(record_batch_stream_writer(stream, x$schema()))
  write_table(x, stream_writer)
}

#' @export
`write_table.arrow::ipc::RecordBatchWriter` <- function(x, stream, ...){
  stream$WriteTable(x)
}

#' @export
`write_table.character` <- function(x, stream, ...) {
  assert_that(length(stream) == 1L)
  write_table(x, fs::path_abs(stream), ...)
}

#' @export
`write_table.fs_path` <- function(x, stream, ...) {
  assert_that(length(stream) == 1L)
  file_stream <- close_on_exit(file_output_stream(stream))
  file_writer <- close_on_exit(record_batch_file_writer(file_stream, x$schema()))
  write_table(x, file_writer, ...)
}

#' @export
`write_table.raw` <- function(x, stream, ...) {
  # how many bytes do we need
  mock <- mock_output_stream()
  write_table(x, mock)
  n <- mock$GetExtentBytesWritten()

  bytes <- raw(n)
  buffer <- buffer(bytes)
  buffer_writer <- fixed_size_buffer_writer(buffer)
  write_table(x, buffer_writer)

  bytes
}

#' Write an object to a stream
#'
#' @param x An object to stream
#' @param stream A stream
#' @param ... additional parameters
#'
#' @export
write_arrow <- function(x, stream, ...){
  UseMethod("write_arrow")
}

#' @export
`write_arrow.arrow::RecordBatch` <- function(x, stream, ...) {
  write_record_batch(x, stream, ...)
}

#' @export
`write_arrow.arrow::Table` <- function(x, stream, ...) {
  write_table(x, stream, ...)
}

#' @export
`write_arrow.data.frame` <- function(x, stream, ...) {
  write_record_batch(record_batch(x), stream, ...)
}
