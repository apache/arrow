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

`arrow::Table` <- R6Class("arrow::Table", inherit = `arrow::Object`,
  public = list(
    num_columns = function() Table__num_columns(self),
    num_rows = function() Table__num_rows(self),
    schema = function() `arrow::Schema`$new(Table__schema(self)),
    to_file = function(path) invisible(Table__to_file(self, fs::path_abs(path))),
    to_stream = function() Table__to_stream(self),
    column = function(i) `arrow::Column`$new(Table__column(self, i))
  )
)

#' Create an arrow::Table from a data frame
#'
#' @param .data a data frame
#'
#' @export
table <- function(.data){
  `arrow::Table`$new(Table__from_dataframe(.data))
}

#' Write a tibble in a binary arrow file
#'
#' @param data a [tibble::tibble]
#' @param path file path
#'
#' @export
write_arrow <- function(data, path){
  table(data)$to_file(path)
}

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
  stream <- file_open(stream); on.exit(stream$Close())
  read_table(stream)
}

#' @export
`read_table.arrow::io::RandomAccessFile` <- function(stream) {
  `arrow::Table`$new(read_table_RandomAccessFile(stream))
}

#' @export
`read_table.arrow::io::BufferReader` <- function(stream) {
  `arrow::Table`$new(read_table_BufferReader(stream))
}

#' @export
`read_table.raw` <- function(stream) {
  stream <- buffer_reader(stream); on.exit(stream$Close())
  read_table(stream)
}

#' @export
`as_tibble.arrow::Table` <- function(x, ...){
  Table__to_dataframe(x)
}

#' Read an tibble from an arrow::Table on disk
#'
#' @param stream input stream
#'
#' @return a [tibble::tibble]
#'
#' @export
read_arrow <- function(stream){
  as_tibble(read_table(stream))
}
