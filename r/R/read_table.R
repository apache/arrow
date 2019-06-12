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

#' Read an [arrow::Table][arrow__Table] from a stream
#'
#' @param stream stream.
#'
#' - a [arrow::ipc::RecordBatchFileReader][arrow__ipc__RecordBatchFileReader]:
#'   read an [arrow::Table][arrow__Table]
#'   from all the record batches in the reader
#'
#' - a [arrow::ipc::RecordBatchStreamReader][arrow__ipc__RecordBatchStreamReader]:
#'   read an [arrow::Table][arrow__Table] from the remaining record batches
#'   in the reader
#'
#'  - a string or [file path][fs::path_abs()]: interpret the file as an arrow
#'    binary file format, and uses a [arrow::ipc::RecordBatchFileReader][arrow__ipc__RecordBatchFileReader]
#'    to process it.
#'
#'  - a raw vector: read using a [arrow::ipc::RecordBatchStreamReader][arrow__ipc__RecordBatchStreamReader]
#'
#' @param use_threads Use threads when converting to a tibble
#'
#' @return
#'
#'  - `read_table` returns an [arrow::Table][arrow__Table]
#'  - `read_arrow` returns a [tibble::tibble()]
#'
#' @details
#'
#' The methods using [arrow::ipc::RecordBatchFileReader][arrow__ipc__RecordBatchFileReader] and
#' [arrow::ipc::RecordBatchStreamReader][arrow__ipc__RecordBatchStreamReader] offer the most
#' flexibility. The other methods are for convenience.
#'
#' @export
read_table <- function(stream){
  UseMethod("read_table")
}

#' @export
`read_table.arrow::ipc::RecordBatchFileReader` <- function(stream) {
  shared_ptr(`arrow::Table`, Table__from_RecordBatchFileReader(stream))
}

#' @export
`read_table.arrow::ipc::RecordBatchStreamReader` <- function(stream) {
  shared_ptr(`arrow::Table`, Table__from_RecordBatchStreamReader(stream))
}

#' @export
read_table.character <- function(stream){
  assert_that(length(stream) == 1L)
  read_table(fs::path_abs(stream))
}

#' @export
read_table.fs_path <- function(stream) {
  stream <- ReadableFile(stream)
  on.exit(stream$close())
  batch_reader <- RecordBatchFileReader(stream)
  shared_ptr(`arrow::Table`, Table__from_RecordBatchFileReader(batch_reader))
}

#' @export
`read_table.raw` <- function(stream) {
  stream <- BufferReader(stream)
  on.exit(stream$close())
  batch_reader <- RecordBatchStreamReader(stream)
  shared_ptr(`arrow::Table`, Table__from_RecordBatchStreamReader(batch_reader))
}

#' @rdname read_table
#' @export
read_arrow <- function(stream, use_threads = TRUE){
  as.data.frame(read_table(stream))
}
