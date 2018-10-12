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

`arrow::ipc::RecordBatchFileWriter` <- R6Class("arrow::ipc::RecordBatchFileWriter", inherit = `arrow::ipc::RecordBatchWriter`)

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
  assert_that(is(schema, "arrow::Schema"))
  `arrow::ipc::RecordBatchFileWriter`$new(ipc___RecordBatchFileWriter__Open(stream, schema))
}

#' @export
`stream.arrow::RecordBatch` <- function(x, stream, ...){
  UseMethod("stream.arrow::RecordBatch", stream)
}

#' @export
`stream.arrow::RecordBatch.arrow::io::OutputStream` <- function(x, stream, allow_64bit = TRUE, ...) {
  file_writer <- close_on_exit(record_batch_file_writer(stream, x$schema()))
  file_writer$WriteRecordBatch(x, allow_64bit)
}

#' @export
`stream.arrow::RecordBatch.character` <- function(x, stream, ...) {
  assert_that(length(stream) == 1L)
  stream(x, fs::path_abs(stream), ...)
}

#' @export
`stream.arrow::RecordBatch.fs_path` <- function(x, stream, ...) {
  assert_that(length(stream) == 1L)
  file_stream <- close_on_exit(file_output_stream(stream))
  stream(x, file_stream, ...)
}

#' @export
`stream.arrow::RecordBatch.raw` <- function(x, stream, ...) {
  # how many bytes do we need
  mock <- mock_output_stream()
  stream(x, mock)
  n <- mock$GetExtentBytesWritten()

  bytes <- raw(n)
  buffer_writer <- fixed_size_buffer_writer(bytes)
  stream(x, buffer_writer)

  bytes
}
