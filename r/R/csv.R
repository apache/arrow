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

#' Read a CSV or other delimited file with Arrow
#'
#' These functions uses the Arrow C++ CSV reader to read into a `data.frame`.
#' Arrow C++ options have been mapped to argument names that follow those of
#' `readr::read_delim()`, and `col_select` was inspired by `vroom::vroom()`.
#'
#' `read_csv_arrow()` and `read_tsv_arrow()` are wrappers around
#' `read_delim_arrow()` that specify a delimiter.
#'
#' Note that not all `readr` options are currently implemented here. Please file
#' an issue if you encounter one that `arrow` should support.
#'
#' If you need to control Arrow-specific reader parameters that don't have an
#' equivalent in `readr::read_csv()`, you can either provide them in the
#' `parse_options`, `convert_options`, or `read_options` arguments, or you can
#' call [csv_table_reader()] directly for lower-level access.
#'
#' @param file A character path to a local file, or an Arrow input stream
#' @param delim Single character used to separate fields within a record.
#' @param quote Single character used to quote strings.
#' @param escape_double Does the file escape quotes by doubling them?
#' i.e. If this option is `TRUE`, the value `""""` represents
#' a single quote, `\"`.
#' @param escape_backslash Does the file use backslashes to escape special
#' characters? This is more general than `escape_double` as backslashes
#' can be used to escape the delimiter character, the quote character, or
#' to add special characters like `\\n`.
#' @param col_names If `TRUE`, the first row of the input will be used as the
#' column names and will not be included in the data frame. (Note that `FALSE`
#' is not currently supported.) Alternatively, you can specify a character
#' vector of column names.
#' @param col_select A character vector of column names to keep, as in the
#' "select" argument to `data.table::fread()`, or a
#' [tidy selection specification][tidyselect::vars_select()]
#' of columns, as used in `dplyr::select()`.
#' @param skip_empty_rows Should blank rows be ignored altogether? If
#' `TRUE`, blank rows will not be represented at all. If `FALSE`, they will be
#' filled with missings.
#' @param skip Number of lines to skip before reading data.
#' @param parse_options see [csv_parse_options()]. If given, this overrides any
#' parsing options provided in other arguments (e.g. `delim`, `quote`, etc.).
#' @param convert_options see [csv_convert_options()]
#' @param read_options see [csv_read_options()]
#' @param as_tibble Should the function return a `data.frame` or an
#' [arrow::Table][arrow__Table]?
#'
#' @return A `data.frame`, or an `arrow::Table` if `as_tibble = FALSE`.
#' @export
#' @examples
#' \donttest{
#' try({
#'   tf <- tempfile()
#'   on.exit(unlink(tf))
#'   write.csv(iris, file = tf)
#'   df <- read_csv_arrow(tf)
#'   dim(df)
#'   # Can select columns
#'   df <- read_csv_arrow(tf, col_select = starts_with("Sepal"))
#' })
#' }
read_delim_arrow <- function(file,
                             delim = ",",
                             quote = '"',
                             escape_double = TRUE,
                             escape_backslash = FALSE,
                             col_names = TRUE,
                             # col_types = TRUE,
                             col_select = NULL,
                             # na = c("", "NA"),
                             # quoted_na = TRUE,
                             skip_empty_rows = TRUE,
                             skip = 0L,
                             parse_options = NULL,
                             convert_options = NULL,
                             read_options = NULL,
                             as_tibble = TRUE) {

  if (identical(col_names, FALSE)) {
    stop("Not implemented", call.=FALSE)
  }
  if (is.null(parse_options)) {
    parse_options <- readr_to_csv_parse_options(
      delim,
      quote,
      escape_double,
      escape_backslash,
      skip_empty_rows
    )
  }

  if (is.null(read_options)) {
    if (isTRUE(col_names)) {
      # C++ default to parse is 0-length string array
      col_names <- character(0)
    }
    read_options <- csv_read_options(
      skip_rows = skip,
      column_names = col_names
    )
  }
  if (is.null(convert_options)) {
    # TODO:
    # * na strings (needs wiring in csv_convert_options)
    # * col_types (needs wiring in csv_convert_options). Note that we can't do
    # col_types if col_names is strings because the column type specification
    # requires a map of name: type, but the CSV reader doesn't handle user-
    # provided names--they're renamed after the fact.
    convert_options <- csv_convert_options()
  }

  reader <- csv_table_reader(
    file,
    read_options = read_options,
    parse_options = parse_options,
    convert_options = convert_options
  )

  tab <- reader$Read()$select(!!enquo(col_select))

  if (isTRUE(as_tibble)) {
    tab <- as.data.frame(tab)
  }

  tab
}

#' @rdname read_delim_arrow
#' @export
read_csv_arrow <- function(file,
                           quote = '"',
                           escape_double = TRUE,
                           escape_backslash = FALSE,
                           col_names = TRUE,
                           # col_types = TRUE,
                           col_select = NULL,
                           # na = c("", "NA"),
                           # quoted_na = TRUE,
                           skip_empty_rows = TRUE,
                           skip = 0L,
                           parse_options = NULL,
                           convert_options = NULL,
                           read_options = NULL,
                           as_tibble = TRUE) {

  mc <- match.call()
  mc$delim <- ","
  mc[[1]] <- as.name("read_delim_arrow")
  eval.parent(mc)
}

#' @rdname read_delim_arrow
#' @export
read_tsv_arrow <- function(file,
                           quote = '"',
                           escape_double = TRUE,
                           escape_backslash = FALSE,
                           col_names = TRUE,
                           # col_types = TRUE,
                           col_select = NULL,
                           # na = c("", "NA"),
                           # quoted_na = TRUE,
                           skip_empty_rows = TRUE,
                           skip = 0L,
                           parse_options = NULL,
                           convert_options = NULL,
                           read_options = NULL,
                           as_tibble = TRUE) {

  mc <- match.call()
  mc$delim <- "\t"
  mc[[1]] <- as.name("read_delim_arrow")
  eval.parent(mc)
}

#' @include R6.R

`arrow::csv::TableReader` <- R6Class("arrow::csv::TableReader", inherit = `arrow::Object`,
  public = list(
    Read = function() shared_ptr(`arrow::Table`, csv___TableReader__Read(self))
  )
)

`arrow::csv::ReadOptions` <- R6Class("arrow::csv::ReadOptions", inherit = `arrow::Object`)
`arrow::csv::ParseOptions` <- R6Class("arrow::csv::ParseOptions", inherit = `arrow::Object`)
`arrow::csv::ConvertOptions` <- R6Class("arrow::csv::ConvertOptions", inherit = `arrow::Object`)

#' Read options for the Arrow file readers
#'
#' @param use_threads Whether to use the global CPU thread pool
#' @param block_size Block size we request from the IO layer; also determines
#' the size of chunks when use_threads is `TRUE`. NB: if `FALSE`, JSON input
#' must end with an empty line.
#' @param skip_rows Number of lines to skip before reading data.
#' @param column_names Character vector to supply column names. If length-0
#' (the default), the first non-skipped row will be parsed to generate column
#' names.
#'
#' @export
csv_read_options <- function(use_threads = option_use_threads(),
                             block_size = 1048576L,
                             skip_rows = 0L,
                             column_names = character(0)) {
  shared_ptr(`arrow::csv::ReadOptions`, csv___ReadOptions__initialize(
    list(
      use_threads = use_threads,
      block_size = block_size,
      skip_rows = skip_rows,
      column_names = column_names
    )
  ))
}

readr_to_csv_parse_options <- function(delim = ",",
                                       quote = '"',
                                       escape_double = TRUE,
                                       escape_backslash = FALSE,
                                       skip_empty_rows = TRUE) {
  # This function translates from the readr argument list to the arrow arg names
  # TODO: validate inputs
  csv_parse_options(
    delimiter = delim,
    quoting = nzchar(quote),
    quote_char = quote,
    double_quote = escape_double,
    escaping = escape_backslash,
    escape_char = '\\',
    newlines_in_values = escape_backslash,
    ignore_empty_lines = skip_empty_rows
  )
}

#' Parsing options for Arrow file readers
#'
#' @param delimiter Field delimiter
#' @param quoting Whether quoting is used
#' @param quote_char Quoting character (if `quoting` is `TRUE`)
#' @param double_quote Whether a quote inside a value is double-quoted
#' @param escaping Whether escaping is used
#' @param escape_char Escaping character (if `escaping` is `TRUE`)
#' @param newlines_in_values Whether values are allowed to contain CR (`0x0d`) and LF (`0x0a`) characters
#' @param ignore_empty_lines Whether empty lines are ignored.  If `FALSE`, an empty line represents
#'
#' @export
csv_parse_options <- function(delimiter = ",",
                              quoting = TRUE,
                              quote_char = '"',
                              double_quote = TRUE,
                              escaping = FALSE,
                              escape_char = '\\',
                              newlines_in_values = FALSE,
                              ignore_empty_lines = TRUE) {

  shared_ptr(`arrow::csv::ParseOptions`, csv___ParseOptions__initialize(
    list(
      delimiter = delimiter,
      quoting = quoting,
      quote_char = quote_char,
      double_quote = double_quote,
      escaping = escaping,
      escape_char = escape_char,
      newlines_in_values = newlines_in_values,
      ignore_empty_lines = ignore_empty_lines
    )
  ))
}

#' Conversion options for the CSV reader
#'
#' @param check_utf8 Whether to check UTF8 validity of string columns
#'
#' @export
csv_convert_options <- function(check_utf8 = TRUE) {
  # TODO: there are more conversion options available:
  # // Optional per-column types (disabling type inference on those columns)
  # std::unordered_map<std::string, std::shared_ptr<DataType>> column_types;
  # // Recognized spellings for null values
  # std::vector<std::string> null_values;
  # // Recognized spellings for boolean values
  # std::vector<std::string> true_values;
  # std::vector<std::string> false_values;
  # // Whether string / binary columns can have null values.
  # // If true, then strings in "null_values" are considered null for string columns.
  # // If false, then all strings are valid string values.
  # bool strings_can_be_null = false;

  shared_ptr(`arrow::csv::ConvertOptions`, csv___ConvertOptions__initialize(
    list(
      check_utf8 = check_utf8
    )
  ))
}

#' Arrow CSV and JSON table readers
#'
#' These methods wrap the Arrow C++ CSV and JSON table readers.
#' For an interface to the CSV reader that's more familiar for R users, see
#' [read_csv_arrow()]
#'
#' @param file A character path to a local file, or an Arrow input stream
#' @param read_options see [csv_read_options()]
#' @param parse_options see [csv_parse_options()]
#' @param convert_options see [csv_convert_options()]
#' @param ... additional parameters.
#'
#' @return An `arrow::csv::TableReader` or `arrow::json::TableReader` R6
#' object. Call `$Read()` on it to get an Arrow Table.
#' @export
csv_table_reader <- function(file,
  read_options = csv_read_options(),
  parse_options = csv_parse_options(),
  convert_options = csv_convert_options(),
  ...
){
  UseMethod("csv_table_reader")
}

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
  csv_table_reader(mmap_open(file),
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
