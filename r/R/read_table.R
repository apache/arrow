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

#' Read an arrow::Table from a stream
#'
#' @param stream stream. Either a stream created by [ReadableFile()] or [mmap_open()] or a file path.
#'
#' @export
read_table <- function(stream){
  UseMethod("read_table")
}

#' @export
read_table.character <- function(stream){
  assert_that(length(stream) == 1L)
  read_table(fs::path_abs(stream))
}

#' @export
read_table.fs_path <- function(stream) {
  stream <- close_on_exit(ReadableFile(stream))
  read_table(stream)
}

#' @export
`read_table.arrow::io::RandomAccessFile` <- function(stream) {
  reader <- RecordBatchFileReader(stream)
  read_table(reader)
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
`read_table.arrow::io::BufferReader` <- function(stream) {
  reader <- RecordBatchStreamReader(stream)
  read_table(reader)
}

#' @export
`read_table.raw` <- function(stream) {
  stream <- close_on_exit(BufferReader(stream))
  read_table(stream)
}

