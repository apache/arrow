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
#' @param x `data.frame` or RecordBatch
#' @param sink A file path or an OutputStream
#'
#' @return the input `x` invisibly.
#'
#' @export
#' @examples
#' \donttest{
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' write_feather(mtcars, tf)
#' }
#' @include arrow-package.R
write_feather <- function(x, sink) {
  x_out <- x

  if (is.data.frame(x)) {
    x <- record_batch(x)
  }
  assert_is(x, "RecordBatch")

  if (is.character(sink)) {
    sink <- FileOutputStream$create(sink)
    on.exit(sink$close())
  }
  assert_is(sink, "OutputStream")

  ipc___WriteFeather__RecordBatch(sink, x)

  invisible(x_out)
}

#' Read a Feather file
#'
#' @param file A character file path, a raw vector, or `InputStream`, passed to
#' `FeatherReader$create()`.
#' @inheritParams read_delim_arrow
#' @param ... additional parameters
#'
#' @return A `data.frame` if `as_data_frame` is `TRUE` (the default), or an
#' [arrow::Table][Table] otherwise
#'
#' @export
#' @examples
#' \donttest{
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' write_feather(iris, tf)
#' df <- read_feather(tf)
#' dim(df)
#' # Can select columns
#' df <- read_feather(tf, col_select = starts_with("Sepal"))
#' }
read_feather <- function(file, col_select = NULL, as_data_frame = TRUE, ...) {
  reader <- FeatherReader$create(file, ...)

  all_columns <- ipc___feather___Reader__column_names(reader)
  col_select <- enquo(col_select)
  columns <- if (!quo_is_null(col_select)) {
    vars_select(all_columns, !!col_select)
  }

  out <- reader$Read(columns)
  if (isTRUE(as_data_frame)) {
    out <- as.data.frame(out)
  }
  out
}

#' @title FeatherReader class
#' @rdname FeatherReader
#' @name FeatherReader
#' @docType class
#' @usage NULL
#' @format NULL
#' @description This class enables you to interact with Feather files. Create
#' one to connect to a file or other InputStream, and call `Read()` on it to
#' make an `arrow::Table`. See its usage in [`read_feather()`].
#'
#' @section Factory:
#'
#' The `FeatherReader$create()` factory method instantiates the object and
#' takes the following arguments:
#'
#' - `file` A character file name, raw vector, or Arrow file connection object
#'    (e.g. `RandomAccessFile`).
#' - `mmap` Logical: whether to memory-map the file (default `TRUE`)
#' - `...` Additional arguments, currently ignored
#'
#' @section Methods:
#'
#' - `$version()`
#' - `$Read(columns)`
#'
#' @export
#' @include arrow-package.R
FeatherReader <- R6Class("FeatherReader", inherit = ArrowObject,
  public = list(
    version = function() ipc___feather___Reader__version(self),
    Read = function(columns) {
      shared_ptr(Table, ipc___feather___Reader__Read(self, columns))
    }
  )
)

FeatherReader$create <- function(file, mmap = TRUE, ...) {
  file <- make_readable_file(file, mmap)
  shared_ptr(FeatherReader, ipc___feather___Reader__Open(file))
}