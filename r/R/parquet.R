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

`parquet::arrow::ParquetFileReader` <- R6Class("parquet::ParquetFileReader",
  inherit = `arrow::Object`,
  public = list(
    Read = function() shared_ptr(`arrow::Table`, parquet___arrow___ParquetFileReader__Read(self))
  )
)

#' @export
parquet_file_reader <- function(file, ...) {
  UseMethod("parquet_file_reader")
}

#' @export
`parquet_file_reader.arrow::io::RandomAccessFile` <- function(file, ...) {
  unique_ptr(`parquet::arrow::ParquetFileReader`, parquet___arrow___ParquetFileReader__OpenFile(file))
}

#' @export
parquet_file_reader.fs_path <- function(file, memory_map = TRUE, ...) {
  if (isTRUE(memory_map)) {
    parquet_file_reader(mmap_open(file), ...)
  } else {
    parquet_file_reader(ReadableFile(file), ...)
  }
}

#' @export
parquet_file_reader.character <- function(file, memory_map = TRUE, ...) {
  parquet_file_reader(fs::path_abs(file), memory_map = memory_map, ...)
}

#' Read Parquet file from disk
#'
#' '[Parquet](https://parquet.apache.org/)' is a columnar storage file format.
#' This function enables you to read Parquet files into R.
#'
#' @param file a file path
#' @param as_tibble Should the [arrow::Table][arrow__Table] be converted to a
#' tibble? Default is `TRUE`.
#' @param ... Additional arguments, currently ignored
#'
#' @return A [arrow::Table][arrow__Table], or a `tbl_df` if `as_tibble` is
#' `TRUE`.
#' @examples
#'
#' \dontrun{
#'   df <- read_parquet(system.file("v0.7.1.parquet", package="arrow"))
#' }
#'
#' @export
read_parquet <- function(file, as_tibble = TRUE, ...) {
  reader <- parquet_file_reader(file, ...)
  tab <- reader$Read()

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
#'
#' \dontrun{
#'   tf <- tempfile(fileext = ".parquet")
#'   write_parquet(tibble::tibble(x = 1:5), tf)
#' }
#'
#' @export
write_parquet <- function(table, file) {
  write_parquet_file(to_arrow(table), file)
}
