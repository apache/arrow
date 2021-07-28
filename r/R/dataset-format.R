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
#'   * `dict_columns`: Names of columns which should be read as dictionaries.
#'   * Any Parquet options from [FragmentScanOptions].
#'
#'   `format = "text"`: see [CsvParseOptions]. Note that you can specify them either
#'   with the Arrow C++ library naming ("delimiter", "quoting", etc.) or the
#'   `readr`-style naming used in [read_csv_arrow()] ("delim", "quote", etc.).
#'   Not all `readr` options are currently supported; please file an issue if
#'   you encounter one that `arrow` should support. Also, the following options are
#'   supported. From [CsvReadOptions]:
#'   * `skip_rows`
#'   * `column_names`
#'   * `autogenerate_column_names`
#'   From [CsvFragmentScanOptions] (these values can be overridden at scan time):
#'   * `convert_options`: a [CsvConvertOptions]
#'   * `block_size`
#'
#' It returns the appropriate subclass of `FileFormat` (e.g. `ParquetFileFormat`)
#' @rdname FileFormat
#' @name FileFormat
#' @examplesIf arrow_with_dataset() && tolower(Sys.info()[["sysname"]]) != "windows"
#' ## Semi-colon delimited files
#' # Set up directory for examples
#' tf <- tempfile()
#' dir.create(tf)
#' on.exit(unlink(tf))
#' write.table(mtcars, file.path(tf, "file1.txt"), sep = ";", row.names = FALSE)
#' 
#' # Create FileFormat object
#' format <- FileFormat$create(format = "text", delimiter = ";")
#' 
#' open_dataset(tf, format = format)
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
ParquetFileFormat$create <- function(...,
                                     dict_columns = character(0)) {
 options <- ParquetFragmentScanOptions$create(...)
 dataset___ParquetFileFormat__Make(options, dict_columns)
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
CsvFileFormat$create <- function(..., opts = csv_file_format_parse_options(...),
                                 convert_options = csv_file_format_convert_options(...),
                                 read_options = csv_file_format_read_options(...)) {
  dataset___CsvFileFormat__Make(opts, convert_options, read_options)
}

# Support both readr-style option names and Arrow C++ option names
csv_file_format_parse_options <- function(...) {
  opts <- list(...)
  # Filter out arguments meant for CsvConvertOptions/CsvReadOptions
  convert_opts <- names(formals(CsvConvertOptions$create))
  read_opts <- names(formals(CsvReadOptions$create))
  opts[convert_opts] <- NULL
  opts[read_opts] <- NULL
  opt_names <- names(opts)
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
    do.call(readr_to_csv_parse_options, opts) # all options have readr-style names
  } else {
    do.call(CsvParseOptions$create, opts) # all options have Arrow C++ names
  }
}

csv_file_format_convert_options <- function(...) {
  opts <- list(...)
  # Filter out arguments meant for CsvParseOptions/CsvReadOptions
  arrow_opts <- names(formals(CsvParseOptions$create))
  readr_opts <- names(formals(readr_to_csv_parse_options))
  read_opts <- names(formals(CsvReadOptions$create))
  opts[arrow_opts] <- NULL
  opts[readr_opts] <- NULL
  opts[read_opts] <- NULL
  do.call(CsvConvertOptions$create, opts)
}

csv_file_format_read_options <- function(...) {
  opts <- list(...)
  # Filter out arguments meant for CsvParseOptions/CsvConvertOptions
  arrow_opts <- names(formals(CsvParseOptions$create))
  readr_opts <- names(formals(readr_to_csv_parse_options))
  convert_opts <- names(formals(CsvConvertOptions$create))
  opts[arrow_opts] <- NULL
  opts[readr_opts] <- NULL
  opts[convert_opts] <- NULL
  do.call(CsvReadOptions$create, opts)
}

#' Format-specific scan options
#'
#' @description
#' A `FragmentScanOptions` holds options specific to a `FileFormat` and a scan
#' operation.
#'
#' @section Factory:
#' `FragmentScanOptions$create()` takes the following arguments:
#' * `format`: A string identifier of the file format. Currently supported values:
#'   * "parquet"
#'   * "csv"/"text", aliases for the same format.
#' * `...`: Additional format-specific options
#'
#'   `format = "parquet"``:
#'   * `use_buffered_stream`: Read files through buffered input streams rather than
#'                            loading entire row groups at once. This may be enabled
#'                            to reduce memory overhead. Disabled by default.
#'   * `buffer_size`: Size of buffered stream, if enabled. Default is 8KB.
#'   * `pre_buffer`: Pre-buffer the raw Parquet data. This can improve performance
#'                   on high-latency filesystems. Disabled by default.
#
#'   `format = "text"`: see [CsvConvertOptions]. Note that options can only be
#'   specified with the Arrow C++ library naming. Also, "block_size" from
#'   [CsvReadOptions] may be given.
#'
#' It returns the appropriate subclass of `FragmentScanOptions`
#' (e.g. `CsvFragmentScanOptions`).
#' @rdname FragmentScanOptions
#' @name FragmentScanOptions
#' @export
FragmentScanOptions <- R6Class("FragmentScanOptions", inherit = ArrowObject,
  active = list(
    # @description
    # Return the `FragmentScanOptions`'s type
    type = function() dataset___FragmentScanOptions__type_name(self)
  )
)
FragmentScanOptions$create <- function(format, ...) {
  opt_names <- names(list(...))
  if (format %in% c("csv", "text", "tsv")) {
    CsvFragmentScanOptions$create(...)
  } else if (format == "parquet") {
    ParquetFragmentScanOptions$create(...)
  } else {
    stop("Unsupported file format: ", format, call. = FALSE)
  }
}

#' @export
as.character.FragmentScanOptions <- function(x, ...) {
  x$type
}

#' @usage NULL
#' @format NULL
#' @rdname FragmentScanOptions
#' @export
CsvFragmentScanOptions <- R6Class("CsvFragmentScanOptions", inherit = FragmentScanOptions)
CsvFragmentScanOptions$create <- function(...,
                                          convert_opts = csv_file_format_convert_options(...),
                                          read_opts = csv_file_format_read_options(...)) {
  dataset___CsvFragmentScanOptions__Make(convert_opts, read_opts)
}

#' @usage NULL
#' @format NULL
#' @rdname FragmentScanOptions
#' @export
ParquetFragmentScanOptions <- R6Class("ParquetFragmentScanOptions", inherit = FragmentScanOptions)
ParquetFragmentScanOptions$create <- function(use_buffered_stream = FALSE,
                                              buffer_size = 8196,
                                              pre_buffer = FALSE) {
  dataset___ParquetFragmentScanOptions__Make(use_buffered_stream, buffer_size, pre_buffer)
}

#' Format-specific write options
#'
#' @description
#' A `FileWriteOptions` holds write options specific to a `FileFormat`.
FileWriteOptions <- R6Class("FileWriteOptions", inherit = ArrowObject,
  public = list(
    update = function(table, ...) {
      if (self$type == "parquet") {
        dataset___ParquetFileWriteOptions__update(self,
            ParquetWriterProperties$create(table, ...),
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
      } else if (self$type == "csv") {
          dataset___CsvFileWriteOptions__update(self,
              CsvWriteOptions$create(...))
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
