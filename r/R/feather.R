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

#' @include arrow-package.R

`arrow::feather::TableWriter` <- R6Class("arrow::feather::TableWriter", inherit = Object,
  public = list(
    SetDescription = function(description) ipc___feather___TableWriter__SetDescription(self, description),
    SetNumRows = function(num_rows) ipc___feather___TableWriter__SetNumRows(self, num_rows),
    Append = function(name, values) ipc___feather___TableWriter__Append(self, name, values),
    Finalize = function() ipc___feather___TableWriter__Finalize(self)
  )
)

`arrow::feather::TableReader` <- R6Class("arrow::feather::TableReader", inherit = Object,
  public = list(
    GetDescription = function() ipc___feather___TableReader__GetDescription(self),
    HasDescription = function() ipc__feather___TableReader__HasDescription(self),
    version = function() ipc___feather___TableReader__version(self),
    num_rows = function() ipc___feather___TableReader__num_rows(self),
    num_columns = function() ipc___feather___TableReader__num_columns(self),
    GetColumnName = function(i) ipc___feather___TableReader__GetColumnName(self, i),
    GetColumn = function(i) shared_ptr(Array, ipc___feather___TableReader__GetColumn(self, i)),
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
FeatherTableWriter.OutputStream <- function(stream){
  unique_ptr(`arrow::feather::TableWriter`, ipc___feather___TableWriter__Open(stream))
}

#' Write data in the Feather format
#'
#' @param data `data.frame` or `arrow::RecordBatch`
#' @param stream A file path or an OutputStream
#'
#' @export
#' @examples
#' \donttest{
#' try({
#'   tf <- tempfile()
#'   on.exit(unlink(tf))
#'   write_feather(mtcars, tf)
#' })
#' }
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
#' @param stream A file path or an OutputStream
#'
#' @export
#' @keywords internal
write_feather_RecordBatch <- function(data, stream) {
  UseMethod("write_feather_RecordBatch", stream)
}

#' @export
`write_feather_RecordBatch.default` <- function(data, stream) {
  stop("unsupported")
}

#' @export
write_feather_RecordBatch.character <- function(data, stream) {
  file_stream <- FileOutputStream$create(stream)
  on.exit(file_stream$close())
  write_feather_RecordBatch.OutputStream(data, file_stream)
}

#' @export
write_feather_RecordBatch.OutputStream <- function(data, stream) {
  ipc___TableWriter__RecordBatch__WriteFeather(FeatherTableWriter(stream), data)
}

#' A `arrow::feather::TableReader` to read from a file
#'
#' @param file A file path or RandomAccessFile
#' @param mmap Is the file memory mapped (applicable to the `character` method)
#' @param ... extra parameters
#'
#' @export
FeatherTableReader <- function(file, mmap = TRUE, ...){
  UseMethod("FeatherTableReader")
}

#' @export
FeatherTableReader.character <- function(file, mmap = TRUE, ...) {
  if (isTRUE(mmap)) {
    stream <- mmap_open(file, ...)
  } else {
    stream <- ReadableFile$create(file, ...)
  }
  FeatherTableReader(stream)
}

#' @export
FeatherTableReader.raw <- function(file, mmap = TRUE, ...) {
  FeatherTableReader(BufferReader$create(file), mmap = mmap, ...)
}

#' @export
FeatherTableReader.RandomAccessFile <- function(file, mmap = TRUE, ...){
  unique_ptr(`arrow::feather::TableReader`, ipc___feather___TableReader__Open(file))
}

#' @export
`FeatherTableReader.arrow::feather::TableReader` <- function(file, mmap = TRUE, ...){
  file
}

#' Read a Feather file
#'
#' @param file an `arrow::feather::TableReader` or whatever the [FeatherTableReader()] function can handle
#' @inheritParams read_delim_arrow
#' @param ... additional parameters
#'
#' @return A `data.frame` if `as_tibble` is `TRUE` (the default), or a [arrow::Table][arrow__Table] otherwise
#'
#' @export
#' @examples
#' \donttest{
#' try({
#'   tf <- tempfile()
#'   on.exit(unlink(tf))
#'   write_feather(iris, tf)
#'   df <- read_feather(tf)
#'   dim(df)
#'   # Can select columns
#'   df <- read_feather(tf, col_select = starts_with("Sepal"))
#' })
#' }
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
