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
#'   `readr`-style naming used in [read_csv_arrow()] ("delim", "quote", etc.).
#'   Not all `readr` options are currently supported; please file an issue if
#'   you encounter one that `arrow` should support.
#'
#' It returns the appropriate subclass of `FileFormat` (e.g. `ParquetFileFormat`)
#' @rdname FileFormat
#' @name FileFormat
#' @export
FileFormat <- R6Class("FileFormat", inherit = ArrowObject,
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
    dataset___IpcFileFormat__Make()
  } else {
    stop("Unsupported file format: ", format, call. = FALSE)
  }
}

#' @export
as.character.FileFormat <- function(x, ...) {
  out <- x$type
  # Slight hack: special case IPC -> feather, otherwise is just the type_name
  ifelse(out == "ipc", "feather", out)
}

#' @usage NULL
#' @format NULL
#' @rdname FileFormat
#' @export
ParquetFileFormat <- R6Class("ParquetFileFormat", inherit = FileFormat)
ParquetFileFormat$create <- function(use_buffered_stream = FALSE,
                                     buffer_size = 8196,
                                     dict_columns = character(0)) {
 dataset___ParquetFileFormat__Make(use_buffered_stream, buffer_size, dict_columns)
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
  dataset___CsvFileFormat__Make(opts)
}

# Support both readr-style option names and Arrow C++ option names
csv_file_format_parse_options <- function(...) {
  opt_names <- names(list(...))
  # Catch any readr-style options specified with full option names that are
  # supported by read_delim_arrow() (and its wrappers) but are not yet
  # supported here
  unsup_readr_opts <- setdiff(
    names(formals(read_delim_arrow)),
    names(formals(readr_to_csv_parse_options))
  )
  is_unsup_opt <- opt_names %in% unsup_readr_opts
  unsup_opts <- opt_names[is_unsup_opt]
  if (length(unsup_opts)) {
    stop(
      "The following ",
      ngettext(length(unsup_opts), "option is ", "options are "),
      "supported in \"read_delim_arrow\" functions ",
      "but not yet supported here: ",
      oxford_paste(unsup_opts),
      call. = FALSE
    )
  }
  # Catch any options with full or partial names that do not match any of the
  # recognized Arrow C++ option names or readr-style option names
  arrow_opts <- names(formals(CsvParseOptions$create))
  readr_opts <- names(formals(readr_to_csv_parse_options))
  is_arrow_opt <- !is.na(pmatch(opt_names, arrow_opts))
  is_readr_opt <- !is.na(pmatch(opt_names, readr_opts))
  unrec_opts <- opt_names[!is_arrow_opt & !is_readr_opt]
  if (length(unrec_opts)) {
    stop(
      "Unrecognized ",
      ngettext(length(unrec_opts), "option", "options"),
      ": ",
      oxford_paste(unrec_opts),
      call. = FALSE
    )
  }
  # Catch options with ambiguous partial names (such as "del") that make it
  # unclear whether the user is specifying Arrow C++ options ("delimiter") or
  # readr-style options ("delim")
  is_ambig_opt <- is.na(pmatch(opt_names, c(arrow_opts, readr_opts)))
  ambig_opts <- opt_names[is_ambig_opt]
  if (length(ambig_opts)) {
    stop("Ambiguous ",
         ngettext(length(ambig_opts), "option", "options"),
         ": ",
         oxford_paste(ambig_opts),
         ". Use full argument names",
         call. = FALSE)
  }
  if (any(is_readr_opt)) {
    # Catch cases when the user specifies a mix of Arrow C++ options and
    # readr-style options
    if (!all(is_readr_opt)) {
      stop("Use either Arrow parse options or readr parse options, not both",
           call. = FALSE)
    }
    readr_to_csv_parse_options(...) # all options have readr-style names
  } else {
    CsvParseOptions$create(...) # all options have Arrow C++ names
  }
}

#' Format-specific write options
#'
#' @description
#' A `FileWriteOptions` holds write options specific to a `FileFormat`.
FileWriteOptions <- R6Class("FileWriteOptions", inherit = ArrowObject,
  public = list(
    update = function(...) {
      if (self$type == "parquet") {
        dataset___ParquetFileWriteOptions__update(self,
            ParquetWriterProperties$create(...),
            ParquetArrowWriterProperties$create(...))
      } else if (self$type == "ipc") {
        args <- list(...)
        if (is.null(args$codec)) {
          dataset___IpcFileWriteOptions__update1(self,
              get_ipc_use_legacy_format(args$use_legacy_format),
              get_ipc_metadata_version(args$metadata_version))
        } else {
          dataset___IpcFileWriteOptions__update2(self,
              get_ipc_use_legacy_format(args$use_legacy_format),
              args$codec,
              get_ipc_metadata_version(args$metadata_version))
        }
      }
      invisible(self)
    }
  ),
  active = list(
    type = function() dataset___FileWriteOptions__type_name(self)
  )
)
FileWriteOptions$create <- function(format, ...) {
  if (!inherits(format, "FileFormat")) {
    format <- FileFormat$create(format)
  }
  options <- dataset___FileFormat__DefaultWriteOptions(format)
  options$update(...)
}
