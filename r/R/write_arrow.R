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

to_arrow <- function(x) {
  UseMethod("to_arrow")
}

`to_arrow.arrow::RecordBatch` <- function(x) x
`to_arrow.arrow::Table` <- function(x) x

# splice the data frame as arguments of table()
# see ?rlang::list2()
`to_arrow.data.frame` <- function(x) table(!!!x)

#' Write Arrow formatted data
#'
#' @param x an [arrow::Table][arrow__Table], an [arrow::RecordBatch][arrow__RecordBatch] or a data.frame
#'
#' @param stream where to serialize to
#'
#' - A [arrow::ipc::RecordBatchWriter][arrow__ipc__RecordBatchWriter]: the `$write()`
#'      of `x` is used. The stream is left open. This uses the streaming format
#'      or the binary file format depending on the type of the writer.
#'
#' - A string or [file path][fs::path_abs()]: `x` is serialized with
#'      a [arrow::ipc::RecordBatchFileWriter][arrow__ipc__RecordBatchFileWriter], i.e.
#'      using the binary file format.
#'
#' - A raw vector: typically of length zero (its data is ignored, and only used for
#'      dispatch). `x` is serialized using the streaming format, i.e. using the
#'      [arrow::ipc::RecordBatchStreamWriter][arrow__ipc__RecordBatchStreamWriter]
#'
#' @param ... extra parameters, currently ignored
#'
#' `write_arrow` is a convenience function, the classes [arrow::ipc::RecordBatchFileWriter][arrow__ipc__RecordBatchFileWriter]
#' and [arrow::ipc::RecordBatchStreamWriter][arrow__ipc__RecordBatchStreamWriter] can be used for more flexibility.
#'
#' @export
write_arrow <- function(x, stream, ...) {
  UseMethod("write_arrow", stream)
}

#' @export
`write_arrow.arrow::ipc::RecordBatchWriter` <- function(x, stream, ...){
  stream$write(x)
}

#' @export
`write_arrow.character` <- function(x, stream, ...) {
  write_arrow(x, fs::path_abs(stream), ...)
}

#' @export
`write_arrow.fs_path` <- function(x, stream, ...) {
  assert_that(length(stream) == 1L)
  x <- to_arrow(x)
  file_stream <- FileOutputStream(stream)
  on.exit(file_stream$close())
  file_writer <- RecordBatchFileWriter(file_stream, x$schema)
  on.exit(file_writer$close(), add = TRUE, after = FALSE)
  write_arrow(x, file_writer, ...)
}

#' @export
`write_arrow.raw` <- function(x, stream, ...) {
  x <- to_arrow(x)
  schema <- x$schema

  # how many bytes do we need
  mock_stream <- MockOutputStream()
  writer <- RecordBatchStreamWriter(mock_stream, schema)
  writer$write(x)
  writer$close()
  n <- mock_stream$GetExtentBytesWritten()

  # now that we know the size, stream in a buffer backed by an R raw vector
  bytes <- raw(n)
  buffer_writer <- FixedSizeBufferWriter(buffer(bytes))
  writer <- RecordBatchStreamWriter(buffer_writer, schema)
  writer$write(x)
  writer$close()

  bytes
}
