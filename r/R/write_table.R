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
  stream_writer <- close_on_exit(RecordBatchStreamWriter(stream, x$schema()))
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
  file_stream <- close_on_exit(FileOutputStream(stream))
  file_writer <- close_on_exit(RecordBatchFileWriter(file_stream, x$schema()))
  write_table(x, file_writer, ...)
}

#' @export
`write_table.raw` <- function(x, stream, ...) {
  # how many bytes do we need
  mock <- MockOutputStream()
  write_table(x, mock)
  n <- mock$GetExtentBytesWritten()

  bytes <- raw(n)
  buffer <- buffer(bytes)
  buffer_writer <- FixedSizeBufferWriter(buffer)
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

#' #' @export
#' `write_arrow.arrow::RecordBatch` <- function(x, stream, ...) {
#'   write_record_batch(x, stream, ...)
#' }

#' @export
`write_arrow.arrow::Table` <- function(x, stream, ...) {
  write_table(x, stream, ...)
}

#' @export
`write_arrow.data.frame` <- function(x, stream, ...) {
  write_table(table(x), stream, ...)
}
