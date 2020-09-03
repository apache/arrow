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

#' Dataset file formats
#'
#' @description
#' A `FileFormat` holds information about how to read and parse the files
#' included in a `Dataset`. There are subclasses corresponding to the supported
#' file formats (`ParquetFileFormat` and `IpcFileFormat`).
#'
#' @section Factory:
#' `FileFormat$create()` takes the following arguments:
#' * `format`: A string identifier of the file format. Currently supported values:
#'   * "parquet"
#'   * "ipc"/"arrow"/"feather", all aliases for each other; for Feather, note that
#'     only version 2 files are supported
#'   * "csv"/"text", aliases for the same thing (because comma is the default
#'     delimiter for text files
#'   * "tsv", equivalent to passing `format = "text", delimiter = "\t"`
#' * `...`: Additional format-specific options
#'
#'   `format = "parquet"``:
#'   * `use_buffered_stream`: Read files through buffered input streams rather than
#'                            loading entire row groups at once. This may be enabled
#'                            to reduce memory overhead. Disabled by default.
#'   * `buffer_size`: Size of buffered stream, if enabled. Default is 8KB.
#'   * `dict_columns`: Names of columns which should be read as dictionaries.
#'
#'   `format = "text"`: see [CsvReadOptions]. Note that you can specify them either
#'   with the Arrow C++ library naming ("delimiter", "quoting", etc.) or the
#'   `readr`-style naming used in [read_csv_arrow()] ("delim", "quote", etc.)
#'
#' It returns the appropriate subclass of `FileFormat` (e.g. `ParquetFileFormat`)
#' @rdname FileFormat
#' @name FileFormat
#' @export
FileFormat <- R6Class("FileFormat", inherit = ArrowObject,
  public = list(
    ..dispatch = function() {
      type <- self$type
      if (type == "parquet") {
        shared_ptr(ParquetFileFormat, self$pointer())
      } else if (type == "ipc") {
        shared_ptr(IpcFileFormat, self$pointer())
      } else if (type == "csv") {
        shared_ptr(CsvFileFormat, self$pointer())
      } else {
        self
      }
    }
  ),
  active = list(
    # @description
    # Return the `FileFormat`'s type
    type = function() dataset___FileFormat__type_name(self)
  )
)
FileFormat$create <- function(format, ...) {
  opt_names <- names(list(...))
  if (format %in% c("csv", "text") || any(opt_names %in% c("delim", "delimiter"))) {
    CsvFileFormat$create(...)
  } else if (format == c("tsv")) {
    CsvFileFormat$create(delimiter = "\t", ...)
  } else if (format == "parquet") {
    ParquetFileFormat$create(...)
  } else if (format %in% c("ipc", "arrow", "feather")) { # These are aliases for the same thing
    shared_ptr(IpcFileFormat, dataset___IpcFileFormat__Make())
  } else {
    stop("Unsupported file format: ", format, call. = FALSE)
  }
}

#' @usage NULL
#' @format NULL
#' @rdname FileFormat
#' @export
ParquetFileFormat <- R6Class("ParquetFileFormat", inherit = FileFormat)
ParquetFileFormat$create <- function(use_buffered_stream = FALSE,
                                     buffer_size = 8196,
                                     dict_columns = character(0),
                                     writer_properties = NULL,
                                     arrow_writer_properties = NULL) {
  if (is.null(writer_properties) && is.null(arrow_writer_properties)) {
    shared_ptr(ParquetFileFormat, dataset___ParquetFileFormat__MakeRead(
      use_buffered_stream, buffer_size, dict_columns))
  } else {
    writer_properties = writer_properties %||% ParquetWriterProperties$create()
    arrow_writer_properties = arrow_writer_properties %||% ParquetArrowWriterProperties$create()
    shared_ptr(ParquetFileFormat, dataset___ParquetFileFormat__MakeWrite(
      writer_properties, arrow_writer_properties))
  }
}

#' @usage NULL
#' @format NULL
#' @rdname FileFormat
#' @export
IpcFileFormat <- R6Class("IpcFileFormat", inherit = FileFormat)

#' @usage NULL
#' @format NULL
#' @rdname FileFormat
#' @export
CsvFileFormat <- R6Class("CsvFileFormat", inherit = FileFormat)
CsvFileFormat$create <- function(..., opts = csv_file_format_parse_options(...)) {
  shared_ptr(CsvFileFormat, dataset___CsvFileFormat__Make(opts))
}

csv_file_format_parse_options <- function(...) {
  # Support both the readr spelling of options and the arrow spelling
  readr_opts <- c("delim", "quote", "escape_double", "escape_backslash", "skip_empty_rows")
  if (any(readr_opts %in% names(list(...)))) {
    readr_to_csv_parse_options(...)
  } else {
    CsvParseOptions$create(...)
  }
}
