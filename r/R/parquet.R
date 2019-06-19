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
  tab <- shared_ptr(`arrow::Table`, read_parquet_file(file))
  if (isTRUE(as_tibble)) {
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
