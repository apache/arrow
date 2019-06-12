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

`arrow::csv::TableReader` <- R6Class("arrow::csv::TableReader", inherit = `arrow::Object`,
  public = list(
    Read = function() shared_ptr(`arrow::Table`, csv___TableReader__Read(self))
  )
)

`arrow::csv::ReadOptions` <- R6Class("arrow::csv::ReadOptions", inherit = `arrow::Object`)
`arrow::csv::ParseOptions` <- R6Class("arrow::csv::ParseOptions", inherit = `arrow::Object`)
`arrow::csv::ConvertOptions` <- R6Class("arrow::csv::ConvertOptions", inherit = `arrow::Object`)

#' read options for the csv reader
#'
#' @param use_threads Whether to use the global CPU thread pool
#' @param block_size Block size we request from the IO layer; also determines the size of chunks when use_threads is `TRUE`
#'
#' @export
csv_read_options <- function(use_threads = TRUE, block_size = 1048576L) {
  shared_ptr(`arrow::csv::ReadOptions`, csv___ReadOptions__initialize(
    list(
      use_threads = use_threads,
      block_size = block_size
    )
  ))
}

#' Parsing options
#'
#' @param delimiter Field delimiter
#' @param quoting Whether quoting is used
#' @param quote_char Quoting character (if `quoting` is `TRUE`)
#' @param double_quote Whether a quote inside a value is double-quoted
#' @param escaping Whether escaping is used
#' @param escape_char Escaping character (if `escaping` is `TRUE`)
#' @param newlines_in_values Whether values are allowed to contain CR (`0x0d`) and LF (`0x0a`) characters
#' @param ignore_empty_lines Whether empty lines are ignored.  If `FALSE`, an empty line represents
#' @param header_rows Number of header rows to skip (including the first row containing column names)
#'
#' @export
csv_parse_options <- function(
  delimiter = ",", quoting = TRUE, quote_char = '"',
  double_quote = TRUE, escaping = FALSE, escape_char = '\\',
  newlines_in_values = FALSE, ignore_empty_lines = TRUE,
  header_rows = 1L
){
  shared_ptr(`arrow::csv::ParseOptions`, csv___ParseOptions__initialize(
    list(
      delimiter = delimiter,
      quoting = quoting,
      quote_char = quote_char,
      double_quote = double_quote,
      escaping = escaping,
      escape_char = escape_char,
      newlines_in_values = newlines_in_values,
      ignore_empty_lines = ignore_empty_lines,
      header_rows = header_rows
    )
  ))
}

#' Conversion Options for the csv reader
#'
#' @param check_utf8 Whether to check UTF8 validity of string columns
#'
#' @export
csv_convert_options <- function(check_utf8 = TRUE){
  shared_ptr(`arrow::csv::ConvertOptions`, csv___ConvertOptions__initialize(
    list(
      check_utf8 = check_utf8
    )
  ))
}

#' CSV table reader
#'
#' @param file file
#' @param read_options, see [csv_read_options()]
#' @param parse_options, see [csv_parse_options()]
#' @param convert_options, see [csv_convert_options()]
#' @param ... additional parameters.
#'
#' @export
csv_table_reader <- function(file,
  read_options = csv_read_options(),
  parse_options = csv_parse_options(),
  convert_options = csv_convert_options(),
  ...
){
  UseMethod("csv_table_reader")
}

#' @importFrom rlang abort
#' @export
csv_table_reader.default <- function(file,
  read_options = csv_read_options(),
  parse_options = csv_parse_options(),
  convert_options = csv_convert_options(),
  ...
) {
  abort("unsupported")
}

#' @export
`csv_table_reader.character` <- function(file,
  read_options = csv_read_options(),
  parse_options = csv_parse_options(),
  convert_options = csv_convert_options(),
  ...
){
  csv_table_reader(fs::path_abs(file),
    read_options = read_options,
    parse_options = parse_options,
    convert_options = convert_options,
    ...
  )
}

#' @export
`csv_table_reader.fs_path` <- function(file,
  read_options = csv_read_options(),
  parse_options = csv_parse_options(),
  convert_options = csv_convert_options(),
  ...
){
  csv_table_reader(ReadableFile(file),
    read_options = read_options,
    parse_options = parse_options,
    convert_options = convert_options,
    ...
  )
}

#' @export
`csv_table_reader.arrow::io::InputStream` <- function(file,
  read_options = csv_read_options(),
  parse_options = csv_parse_options(),
  convert_options = csv_convert_options(),
  ...
){
  shared_ptr(`arrow::csv::TableReader`,
    csv___TableReader__Make(file, read_options, parse_options, convert_options)
  )
}

#' @export
`csv_table_reader.arrow::csv::TableReader` <- function(file,
  read_options = csv_read_options(),
  parse_options = csv_parse_options(),
  convert_options = csv_convert_options(),
  ...
){
  file
}

#' Read csv file into an arrow::Table
#'
#' Use arrow::csv::TableReader from [csv_table_reader()]
#'
#' @param ... Used to construct an arrow::csv::TableReader
#' @export
read_csv_arrow <- function(...) {
  csv_table_reader(...)$Read()
}
