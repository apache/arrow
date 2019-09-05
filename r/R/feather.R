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
#' @include arrow-package.R
write_feather <- function(data, stream) {
  if (is.data.frame(data)) {
    data <- record_batch(data)
  }
  assert_that(inherits(data, "arrow::RecordBatch"))

  if (is.character(stream)) {
    stream <- FileOutputStream$create(stream)
    on.exit(stream$close())
  }
  assert_that(inherits(stream, "OutputStream"))

  writer <- FeatherTableWriter$create(stream)
  ipc___TableWriter__RecordBatch__WriteFeather(writer, data)
}

FeatherTableWriter <- R6Class("FeatherTableWriter", inherit = Object,
  public = list(
    SetDescription = function(description) ipc___feather___TableWriter__SetDescription(self, description),
    SetNumRows = function(num_rows) ipc___feather___TableWriter__SetNumRows(self, num_rows),
    Append = function(name, values) ipc___feather___TableWriter__Append(self, name, values),
    Finalize = function() ipc___feather___TableWriter__Finalize(self)
  )
)

FeatherTableWriter$create <- function(stream) {
  assert_that(inherits(stream, "OutputStream"))
  unique_ptr(FeatherTableWriter, ipc___feather___TableWriter__Open(stream))
}

#' Read a Feather file
#'
#' @param file an FeatherTableReader or whatever the [FeatherTableReader()] function can handle
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
  reader <- FeatherTableReader$create(file, ...)

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

FeatherTableReader <- R6Class("FeatherTableReader", inherit = Object,
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

FeatherTableReader$create <- function(stream, mmap = TRUE, ...) {
  if (is.character(stream)) {
    if (isTRUE(mmap)) {
      stream <- mmap_open(stream, ...)
    } else {
      stream <- ReadableFile$create(stream, ...)
    }
  } else if (is.raw(stream)) {
    stream <- BufferReader$create(stream)
  }

  assert_that(inherits(stream, "InputStream"))
  unique_ptr(FeatherTableReader, ipc___feather___TableReader__Open(stream))
}
