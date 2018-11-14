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

`arrow::ipc::feather::TableWriter` <- R6Class("arrow::ipc::feather::TableWriter", inherit = `arrow::Object`,
  public = list(
    SetDescription = function(description) ipc___feather___TableWriter__SetDescription(self, description),
    SetNumRows = function(num_rows) ipc___feather___TableWriter__SetNumRows(self, num_rows),
    Append = function(name, values) ipc___feather___TableWriter__Append(self, name, values),
    Finalize = function() ipc___feather___TableWriter__Finalize(self)
  )
)

`arrow::ipc::feather::TableReader` <- R6Class("arrow::ipc::feather::TableReader", inherit = `arrow::Object`,
  public = list(
    GetDescription = function() ipc___feather___TableReader__GetDescription(self),
    HasDescription = function() ipc__feather___TableReader__HasDescription(self),
    version = function() ipc___feather___TableReader__version(self),
    num_rows = function() ipc___feather___TableReader__num_rows(self),
    num_columns = function() ipc___feather___TableReader__num_columns(self),
    GetColumnName = function(i) ipc___feather___TableReader__GetColumnName(self, i),
    GetColumn = function(i) shared_ptr(`arrow::Column`, ipc___feather___TableReader__GetColumn(self, i)),
    Read = function() shared_ptr(`arrow::Table`, ipc___feather___TableReader__Read(self))
  )
)

#' Create TableWriter that writes into a stream
#'
#' @param stream an OutputStream
#'
#' @export
feather_table_writer <- function(stream) {
  UseMethod("feather_table_writer")
}

#' @export
`feather_table_writer.arrow::io::OutputStream` <- function(stream){
  unique_ptr(`arrow::ipc::feather::TableWriter`, ipc___feather___TableWriter__Open(stream))
}

#' Write data in the feather format
#'
#' @param data frame or arrow::RecordBatch
#' @param stream A file path or an arrow::io::OutputStream
#'
#' @export
write_feather <- function(data, stream) {
  UseMethod("write_feather", data)
}

#' @export
write_feather.default <- function(data, stream) {
  stop("unsupported")
}

#' @export
write_feather.data.frame <- function(data, stream) {
  write_feather(record_batch(data), stream)
}

#' @method write_feather arrow::RecordBatch
#' @export
`write_feather.arrow::RecordBatch` <- function(data, stream) {
  write_feather_RecordBatch(data, stream)
}

#' @rdname write_feather
#' @export
write_feather_RecordBatch <- function(data, stream) {
  UseMethod("write_feather_RecordBatch", stream)
}

#' @export
#' @method write_feather_RecordBatch default
`write_feather_RecordBatch.default` <- function(data, stream) {
  stop("unsupported")
}

#' @export
#' @method write_feather_RecordBatch character
`write_feather_RecordBatch.character` <- function(data, stream) {
  `write_feather_RecordBatch.fs_path`(data, fs::path_abs(stream))
}

#' @export
#' @method write_feather_RecordBatch fs_path
`write_feather_RecordBatch.fs_path` <- function(data, stream) {
  file_stream <- close_on_exit(file_output_stream(stream))
  `write_feather_RecordBatch.arrow::io::OutputStream`(data, file_stream)
}

#' @export
#' @method write_feather_RecordBatch arrow::io::OutputStream
`write_feather_RecordBatch.arrow::io::OutputStream` <- function(data, stream) {
  ipc___TableWriter__RecordBatch__WriteFeather(feather_table_writer(stream), data)
}

#' A arrow::ipc::feather::TableReader to read from a file
#'
#' @param file A file path, arrow::io::RandomAccessFile
#' @param mmap Is the file memory mapped (applicable to the character and fs_path methods)
#' @param ... extra parameters
#'
#' @export
feather_table_reader <- function(file, mmap = TRUE, ...){
  UseMethod("feather_table_reader")
}

#' @export
feather_table_reader.default <- function(file, mmap = TRUE, ...) {
  stop("unsupported")
}

#' @export
feather_table_reader.character <- function(file, mmap = TRUE, ...) {
  feather_table_reader(fs::path_abs(file), mmap = mmap, ...)
}

#' @export
feather_table_reader.fs_path <- function(file, mmap = TRUE, ...) {
  stream <- if(isTRUE(mmap)) mmap_open(file, ...) else file_open(file, ...)
  feather_table_reader(stream)
}

#' @export
`feather_table_reader.arrow::io::RandomAccessFile` <- function(file, mmap = TRUE, ...){
  unique_ptr(`arrow::ipc::feather::TableReader`, ipc___feather___TableReader__Open(file))
}

#' @export
`feather_table_reader.arrow::ipc::feather::TableReader` <- function(file, mmap = TRUE, ...){
  file
}

#' Read a feather file
#'
#' @param file a arrow::ipc::feather::TableReader or whatever the [feather_table_reader()] function can handle
#' @param ... additional parameters
#'
#' @return an arrow::Table
#'
#' @export
read_feather <- function(file, ...){
  feather_table_reader(file, ...)$Read()
}
