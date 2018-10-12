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
#' @include stream.R

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
  UseMethod("record_batch_file_writer")
}

#' @export
`record_batch_file_writer.arrow::io::OutputStream` <- function(stream, schema) {
  assert_that(inherits(schema, "arrow::Schema"))
  `arrow::ipc::RecordBatchFileWriter`$new(ipc___RecordBatchFileWriter__Open(stream, schema))
}

#' Create a record batch writer that writes to the given stream
#'
#' @param stream an stream
#' @param schema a Schema
#'
#' @export
record_batch_stream_writer <- function(stream, schema) {
  UseMethod("record_batch_stream_writer")
}

#' @method record_batch_stream_writer "arrow::io::OutputStream"
#' @export
`record_batch_stream_writer.arrow::io::OutputStream` <- function(stream, schema) {
  assert_that(inherits(schema, "arrow::Schema"))
  `arrow::ipc::RecordBatchStreamWriter`$new(ipc___RecordBatchStreamWriter__Open(stream, schema))
}

#-------- stream RecordBatch

#' @method "stream" "arrow::RecordBatch"
#' @rdname stream
#' @export
`stream.arrow::RecordBatch` <- function(x, stream, ...){
  UseMethod("stream.arrow::RecordBatch", stream)
}

#' @method "stream.arrow::RecordBatch" "arrow::io::OutputStream"
#' @export
`stream.arrow::RecordBatch.arrow::io::OutputStream` <- function(x, stream, ...) {
  stream_writer <- close_on_exit(record_batch_stream_writer(stream, x$schema()))
  stream(x, stream_writer)
}

#' @method "stream.arrow::RecordBatch" "arrow::ipc::RecordBatchWriter"
#' @export
`stream.arrow::RecordBatch.arrow::ipc::RecordBatchWriter` <- function(x, stream, allow_64bit = TRUE, ...){
  stream$WriteRecordBatch(x, allow_64bit)
}

#' @method "stream.arrow::RecordBatch" character
#' @export
`stream.arrow::RecordBatch.character` <- function(x, stream, ...) {
  assert_that(length(stream) == 1L)
  stream(x, fs::path_abs(stream), ...)
}

#' @method "stream.arrow::RecordBatch" "fs_path"
#' @export
`stream.arrow::RecordBatch.fs_path` <- function(x, stream, ...) {
  assert_that(length(stream) == 1L)
  file_stream <- close_on_exit(file_output_stream(stream))
  file_writer <- close_on_exit(record_batch_file_writer(file_stream, x$schema()))
  stream(x, file_writer, ...)
}

#' @method "stream.arrow::RecordBatch" "raw"
#' @export
`stream.arrow::RecordBatch.raw` <- function(x, stream, ...) {
  # how many bytes do we need
  mock <- mock_output_stream()
  stream(x, mock)
  n <- mock$GetExtentBytesWritten()

  bytes <- raw(n)
  buffer <- buffer(bytes)
  buffer_writer <- fixed_size_buffer_writer(buffer)
  stream(x, buffer_writer)

  bytes
}

#-------- stream Table

#' @method "stream" "arrow::Table"
#' @rdname stream
#' @export
`stream.arrow::Table` <- function(x, stream, ...) {
  UseMethod("stream.arrow::Table", stream)
}

#' @method "stream.arrow::Table" "arrow::io::OutputStream"
#' @export
`stream.arrow::Table.arrow::io::OutputStream` <- function(x, stream, ...) {
  stream_writer <- close_on_exit(record_batch_stream_writer(stream, x$schema()))
  stream(x, stream_writer)
}

#' @method "stream.arrow::Table" "arrow::ipc::RecordBatchWriter"
#' @export
`stream.arrow::Table.arrow::ipc::RecordBatchWriter` <- function(x, stream, ...){
  stream$WriteTable(x)
}

#' @method "stream.arrow::Table" "character"
#' @export
`stream.arrow::Table.character` <- function(x, stream, ...) {
  assert_that(length(stream) == 1L)
  stream(x, fs::path_abs(stream), ...)
}

#' @method "stream.arrow::Table" "fs_path"
#' @export
`stream.arrow::Table.fs_path` <- function(x, stream, ...) {
  assert_that(length(stream) == 1L)
  file_stream <- close_on_exit(file_output_stream(stream))
  file_writer <- close_on_exit(record_batch_file_writer(file_stream, x$schema()))
  stream(x, file_writer, ...)
}

#' @method "stream.arrow::Table" "raw"
#' @export
`stream.arrow::Table.raw` <- function(x, stream, ...) {
  # how many bytes do we need
  mock <- mock_output_stream()
  stream(x, mock)
  n <- mock$GetExtentBytesWritten()

  bytes <- raw(n)
  buffer <- buffer(bytes)
  buffer_writer <- fixed_size_buffer_writer(buffer)
  stream(x, buffer_writer)

  bytes
}
