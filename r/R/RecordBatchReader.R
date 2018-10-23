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

`arrow::RecordBatchReader` <- R6Class("arrow::RecordBatchReader", inherit = `arrow::Object`,
  public = list(
    schema = function() construct(`arrow::Schema`, RecordBatchReader__schema(self)),
    ReadNext = function() {
      construct(`arrow::RecordBatch`, RecordBatchReader__ReadNext(self))
    }
  )
)

`arrow::ipc::RecordBatchStreamReader` <- R6Class("arrow::ipc::RecordBatchStreamReader", inherit = `arrow::RecordBatchReader`)

`arrow::ipc::RecordBatchFileReader` <- R6Class("arrow::ipc::RecordBatchFileReader", inherit = `arrow::Object`,
  public = list(
    schema = function() construct(`arrow::Schema`, ipc___RecordBatchFileReader__schema(self)),
    num_record_batches = function() ipc___RecordBatchFileReader__num_record_batches(self),
    ReadRecordBatch = function(i) construct(`arrow::RecordBatch`, ipc___RecordBatchFileReader__ReadRecordBatch(self, i))
  )
)


#' Create a `arrow::ipc::RecordBatchStreamReader` from an input stream
#'
#' @param stream input stream
#' @export
record_batch_stream_reader <- function(stream){
  UseMethod("record_batch_stream_reader")
}

#' @export
`record_batch_stream_reader.arrow::io::InputStream` <- function(stream) {
  construct(`arrow::ipc::RecordBatchStreamReader`, ipc___RecordBatchStreamReader__Open(stream))
}

#' @export
`record_batch_stream_reader.raw` <- function(stream) {
  record_batch_stream_reader(buffer_reader(stream))
}


#' Create an `arrow::ipc::RecordBatchFileReader` from a file
#'
#' @param file The file to read from
#'
#' @export
record_batch_file_reader <- function(file) {
  UseMethod("record_batch_file_reader")
}

#' @export
`record_batch_file_reader.arrow::io::RandomAccessFile` <- function(file) {
  construct(`arrow::ipc::RecordBatchFileReader`, ipc___RecordBatchFileReader__Open(file))
}

#' @export
`record_batch_file_reader.character` <- function(file) {
  assert_that(length(file) == 1L)
  record_batch_file_reader(fs::path_abs(file))
}

#' @export
`record_batch_file_reader.fs_path` <- function(file) {
  record_batch_file_reader(file_open(file))
}

#-------- read_record_batch

#' Read a single record batch from a stream
#'
#' @param stream input stream
#' @param ... additional parameters
#'
#' @details `stream` can be a `arrow::io::RandomAccessFile` stream as created by [file_open()] or [mmap_open()] or a path.
#'
#' @export
read_record_batch <- function(stream, ...){
  UseMethod("read_record_batch")
}

#' @export
read_record_batch.character <- function(stream, ...){
  assert_that(length(stream) == 1L)
  read_record_batch(fs::path_abs(stream))
}

#' @export
read_record_batch.fs_path <- function(stream, ...){
  stream <- close_on_exit(file_open(stream))
  read_record_batch(stream)
}

#' @export
`read_record_batch.arrow::io::RandomAccessFile` <- function(stream, ...){
  reader <- record_batch_file_reader(stream)
  reader$ReadRecordBatch(0)
}

#' @export
`read_record_batch.arrow::io::BufferReader` <- function(stream, ...){
  reader <- record_batch_stream_reader(stream)
  reader$ReadNext()
}

#' @export
read_record_batch.raw <- function(stream, ...){
  stream <- close_on_exit(buffer_reader(stream))
  read_record_batch(stream)
}

#' @export
`read_record_batch.arrow::ipc::RecordBatchStreamReader` <- function(stream, ...) {
  stream$ReadNext()
}

#' @export
`read_record_batch.arrow::ipc::RecordBatchFileReader` <- function(stream, i = 0, ...) {
  stream$ReadRecordBatch(i)
}

#--------- read_table

#' Read an arrow::Table from a stream
#'
#' @param stream stream. Either a stream created by [file_open()] or [mmap_open()] or a file path.
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
  stream <- close_on_exit(file_open(stream))
  read_table(stream)
}

#' @export
`read_table.arrow::io::RandomAccessFile` <- function(stream) {
  reader <- record_batch_file_reader(stream)
  read_table(reader)
}

#' @export
`read_table.arrow::ipc::RecordBatchFileReader` <- function(stream) {
  construct(`arrow::Table`, Table__from_RecordBatchFileReader(stream))
}

#' @export
`read_table.arrow::ipc::RecordBatchStreamReader` <- function(stream) {
  construct(`arrow::Table`, Table__from_RecordBatchStreamReader(stream))
}

#' @export
`read_table.arrow::io::BufferReader` <- function(stream) {
  reader <- record_batch_stream_reader(stream)
  read_table(reader)
}

#' @export
`read_table.raw` <- function(stream) {
  stream <- close_on_exit(buffer_reader(stream))
  read_table(stream)
}

