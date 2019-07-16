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
    Read = function(columns) {
      shared_ptr(`arrow::Table`, ipc___feather___TableReader__Read(self, columns))
    }
  )
)

#' Create `TableWriter` that writes into a stream
#'
#' @param stream an `OutputStream`
#'
#' @export
FeatherTableWriter <- function(stream) {
  UseMethod("FeatherTableWriter")
}

#' @export
`FeatherTableWriter.arrow::io::OutputStream` <- function(stream){
  unique_ptr(`arrow::ipc::feather::TableWriter`, ipc___feather___TableWriter__Open(stream))
}

#' Write data in the Feather format
#'
#' @param data `data.frame` or `arrow::RecordBatch`
#' @param stream A file path or an `arrow::io::OutputStream`
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

#' Write a record batch in the feather format
#'
#' @param data `data.frame` or `arrow::RecordBatch`
#' @param stream A file path or an `arrow::io::OutputStream`
#'
#' @export
#' @keywords internal
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
  file_stream <- FileOutputStream(stream)
  on.exit(file_stream$close())
  `write_feather_RecordBatch.arrow::io::OutputStream`(data, file_stream)
}

#' @export
#' @method write_feather_RecordBatch arrow::io::OutputStream
`write_feather_RecordBatch.arrow::io::OutputStream` <- function(data, stream) {
  ipc___TableWriter__RecordBatch__WriteFeather(FeatherTableWriter(stream), data)
}

#' A `arrow::ipc::feather::TableReader` to read from a file
#'
#' @param file A file path or `arrow::io::RandomAccessFile`
#' @param mmap Is the file memory mapped (applicable to the `character` and `fs_path` methods)
#' @param ... extra parameters
#'
#' @export
FeatherTableReader <- function(file, mmap = TRUE, ...){
  UseMethod("FeatherTableReader")
}

#' @export
FeatherTableReader.default <- function(file, mmap = TRUE, ...) {
  stop("unsupported")
}

#' @export
FeatherTableReader.character <- function(file, mmap = TRUE, ...) {
  FeatherTableReader(fs::path_abs(file), mmap = mmap, ...)
}

#' @export
FeatherTableReader.fs_path <- function(file, mmap = TRUE, ...) {
  if (isTRUE(mmap)) {
    stream <- mmap_open(file, ...)
  } else {
    stream <- ReadableFile(file, ...)
  }
  FeatherTableReader(stream)
}

#' @export
`FeatherTableReader.arrow::io::RandomAccessFile` <- function(file, mmap = TRUE, ...){
  unique_ptr(`arrow::ipc::feather::TableReader`, ipc___feather___TableReader__Open(file))
}

#' @export
`FeatherTableReader.arrow::ipc::feather::TableReader` <- function(file, mmap = TRUE, ...){
  file
}

#' Read a Feather file
#'
#' @param file an `arrow::ipc::feather::TableReader` or whatever the [FeatherTableReader()] function can handle
#' @param col_select [tidy selection][tidyselect::vars_select()] of columns to read.
#' @param as_tibble should the [arrow::Table][arrow__Table] be converted to a tibble.
#' @param ... additional parameters
#'
#' @return A `data.frame` if `as_tibble` is `TRUE` (the default), or a [arrow::Table][arrow__Table] otherwise
#'
#' @export
read_feather <- function(file, col_select = NULL, as_tibble = TRUE, ...){
  reader <- FeatherTableReader(file, ...)

  all_columns <- ipc___feather___TableReader__column_names(reader)
  col_select <- enquo(col_select)
  columns <- if (!quo_is_null(col_select)) {
    vars_select(all_columns, !!col_select)
  }

  out <- reader$Read(columns)
  if (isTRUE(as_tibble)) {
    out <- as.data.frame(out)
  }
  out
}
