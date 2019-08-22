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

`parquet::arrow::FileReader` <- R6Class("parquet::arrow::FileReader",
  inherit = `arrow::Object`,
  public = list(
    ReadTable = function(col_select = NULL) {
      col_select <- enquo(col_select)
      if(quo_is_null(col_select)) {
        shared_ptr(`arrow::Table`, parquet___arrow___FileReader__ReadTable1(self))
      } else {
        all_vars <- shared_ptr(`arrow::Schema`, parquet___arrow___FileReader__GetSchema(self))$names
        indices <- match(vars_select(all_vars, !!col_select), all_vars) - 1L
        shared_ptr(`arrow::Table`, parquet___arrow___FileReader__ReadTable2(self, indices))
      }
    },
    GetSchema = function() {
      shared_ptr(`arrow::Schema`, parquet___arrow___FileReader__GetSchema(self))
    }
  )
)

`parquet::arrow::ArrowReaderProperties` <- R6Class("parquet::arrow::ArrowReaderProperties",
  inherit = `arrow::Object`,
  public = list(
    read_dictionary = function(column_index) {
      parquet___arrow___ArrowReaderProperties__get_read_dictionary(self, column_index)
    },
    set_read_dictionary = function(column_index, read_dict) {
      parquet___arrow___ArrowReaderProperties__set_read_dictionary(self, column_index, read_dict)
    }
  ),
  active = list(
    use_threads = function(use_threads) {
      if(missing(use_threads)) {
        parquet___arrow___ArrowReaderProperties__get_use_threads(self)
      } else {
        parquet___arrow___ArrowReaderProperties__set_use_threads(self, use_threads)
      }
    }
  )
)

#' Create a new ArrowReaderProperties instance
#'
#' @param use_threads use threads?
#'
#' @export
#' @keywords internal
parquet_arrow_reader_properties <- function(use_threads = option_use_threads()) {
  shared_ptr(`parquet::arrow::ArrowReaderProperties`, parquet___arrow___ArrowReaderProperties__Make(isTRUE(use_threads)))
}

#' Parquet file reader
#'
#' @inheritParams read_delim_arrow
#' @param props reader file properties, as created by [parquet_arrow_reader_properties()]
#'
#' @param ... additional parameters
#'
#' @export
parquet_file_reader <- function(file, props = parquet_arrow_reader_properties(), ...) {
  UseMethod("parquet_file_reader")
}

#' @export
`parquet_file_reader.arrow::io::RandomAccessFile` <- function(file, props = parquet_arrow_reader_properties(), ...) {
  unique_ptr(`parquet::arrow::FileReader`, parquet___arrow___FileReader__OpenFile(file, props))
}

#' @export
parquet_file_reader.character <- function(file,
                                          props = parquet_arrow_reader_properties(),
                                          memory_map = TRUE,
                                          ...) {
  file <- normalizePath(file)
  if (isTRUE(memory_map)) {
    parquet_file_reader(mmap_open(file), props = props, ...)
  } else {
    parquet_file_reader(ReadableFile(file), props = props, ...)
  }
}

#' @export
parquet_file_reader.raw <- function(file, props = parquet_arrow_reader_properties(), ...) {
  parquet_file_reader(BufferReader(file), props = props, ...)
}

#' Read a Parquet file
#'
#' '[Parquet](https://parquet.apache.org/)' is a columnar storage file format.
#' This function enables you to read Parquet files into R.
#'
#' @inheritParams read_delim_arrow
#' @inheritParams parquet_file_reader
#'
#' @return A [arrow::Table][arrow__Table], or a `data.frame` if `as_tibble` is
#' `TRUE`.
#' @examples
#' \donttest{
#' try({
#'   df <- read_parquet(system.file("v0.7.1.parquet", package="arrow"))
#' })
#' }
#' @export
read_parquet <- function(file, col_select = NULL, as_tibble = TRUE, props = parquet_arrow_reader_properties(), ...) {
  reader <- parquet_file_reader(file, props = props, ...)
  tab <- reader$ReadTable(!!enquo(col_select))

  if (as_tibble) {
    tab <- as.data.frame(tab)
  }
  tab
}

#' Write Parquet file to disk
#'
#' [Parquet](https://parquet.apache.org/) is a columnar storage file format.
#' This function enables you to write Parquet files from R.
#'
#' @param table An [arrow::Table][arrow__Table], or an object convertible to it
#' @param file a file path
#'
#' @examples
#' \donttest{
#' try({
#'   tf <- tempfile(fileext = ".parquet")
#'   on.exit(unlink(tf))
#'   write_parquet(tibble::tibble(x = 1:5), tf)
#' })
#' }
#' @export
write_parquet <- function(table, file) {
  write_parquet_file(to_arrow(table), file)
}
