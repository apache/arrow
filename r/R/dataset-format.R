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
#'   `format = "parquet"`:
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
#'   * `column_names`. Note that if a [Schema] is specified, `column_names` must match those specified in the schema.
#'   * `autogenerate_column_names`
#'   From [CsvFragmentScanOptions] (these values can be overridden at scan time):
#'   * `convert_options`: a [CsvConvertOptions]
#'   * `block_size`
#'
#' It returns the appropriate subclass of `FileFormat` (e.g. `ParquetFileFormat`)
#' @rdname FileFormat
#' @name FileFormat
#' @examplesIf arrow_with_dataset()
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
FileFormat <- R6Class("FileFormat",
  inherit = ArrowObject,
  active = list(
    # @description
    # Return the `FileFormat`'s type
    type = function() dataset___FileFormat__type_name(self)
  )
)
FileFormat$create <- function(format, schema = NULL, ...) {
  opt_names <- names(list(...))
  if (format %in% c("csv", "text") || any(opt_names %in% c("delim", "delimiter"))) {
    CsvFileFormat$create(schema = schema, ...)
  } else if (format == c("tsv")) {
    CsvFileFormat$create(delimiter = "\t", schema = schema, ...)
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

#' CSV dataset file format
#'
#' @description
#' A `CSVFileFormat` is a [FileFormat] subclass which holds information about how to
#' read and parse the files included in a CSV `Dataset`.
#'
#' @section Factory:
#' `CSVFileFormat$create()` can take options in the form of lists passed through as `parse_options`,
#'  `read_options`, or `convert_options` parameters.  Alternatively, readr-style options can be passed
#'  through individually.  While it is possible to pass in `CSVReadOptions`, `CSVConvertOptions`, and `CSVParseOptions`
#'  objects, this is not recommended as options set in these objects are not validated for compatibility.
#'
#' @return A `CsvFileFormat` object
#' @rdname CsvFileFormat
#' @name CsvFileFormat
#' @seealso [FileFormat]
#' @examplesIf arrow_with_dataset()
#' # Set up directory for examples
#' tf <- tempfile()
#' dir.create(tf)
#' on.exit(unlink(tf))
#' df <- data.frame(x = c("1", "2", "NULL"))
#' write.table(df, file.path(tf, "file1.txt"), sep = ",", row.names = FALSE)
#'
#' # Create CsvFileFormat object with Arrow-style null_values option
#' format <- CsvFileFormat$create(convert_options = list(null_values = c("", "NA", "NULL")))
#' open_dataset(tf, format = format)
#'
#' # Use readr-style options
#' format <- CsvFileFormat$create(na = c("", "NA", "NULL"))
#' open_dataset(tf, format = format)
#'
#' @export
CsvFileFormat <- R6Class("CsvFileFormat", inherit = FileFormat)
CsvFileFormat$create <- function(...) {
  dots <- list(...)
  options <- check_csv_file_format_args(dots)
  check_schema(options[["schema"]], options[["read_options"]]$column_names)

  dataset___CsvFileFormat__Make(options$parse_options, options$convert_options, options$read_options)
}

# Check all arguments are valid
check_csv_file_format_args <- function(args) {
  options <- list(
    parse_options = args$parse_options,
    convert_options = args$convert_options,
    read_options = args$read_options,
    schema = args$schema
  )

  check_unsupported_args(args)
  check_unrecognised_args(args)

  # Evaluate parse_options first to catch any unsupported arguments
  if (is.null(args$parse_options)) {
    options$parse_options <- do.call(csv_file_format_parse_opts, args)
  } else if (is.list(args$parse_options)) {
    options$parse_options <- do.call(CsvParseOptions$create, args$parse_options)
  }

  if (is.null(args$convert_options)) {
    options$convert_options <- do.call(csv_file_format_convert_opts, args)
  } else if (is.list(args$convert_options)) {
    options$convert_options <- do.call(CsvConvertOptions$create, args$convert_options)
  }

  if (is.null(args$read_options)) {
    options$read_options <- do.call(csv_file_format_read_opts, args)
  } else if (is.list(args$read_options)) {
    options$read_options <- do.call(CsvReadOptions$create, args$read_options)
  }

  options
}

check_unsupported_args <- function(args) {
  opt_names <- get_opt_names(args)

  # Filter out arguments meant for CsvConvertOptions/CsvReadOptions
  supported_convert_opts <- c(names(formals(CsvConvertOptions$create)), "na")

  supported_read_opts <- c(
    names(formals(CsvReadOptions$create)),
    names(formals(readr_to_csv_read_options))
  )

  # We only currently support all of the readr options for parseoptions
  supported_parse_opts <- c(
    names(formals(CsvParseOptions$create)),
    names(formals(readr_to_csv_parse_options))
  )

  # Catch any readr-style options specified with full option names that are
  # supported by read_delim_arrow() (and its wrappers) but are not yet
  # supported here
  unsup_readr_opts <- setdiff(
    names(formals(read_delim_arrow)),
    c(supported_convert_opts, supported_read_opts, supported_parse_opts, "schema")
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
}

# unlists "parse_options", "convert_options", "read_options" and returns them along with
# names of options passed in individually via args.  `get_opt_names()` ignores any
# CSV*Options objects passed in as these are not validated - users must ensure they've
# chosen reasonable values in this case.
get_opt_names <- function(args) {
  opt_names <- names(args)

  # extract names of parse_options, read_options, and convert_options
  if ("parse_options" %in% names(args) && is.list(args[["parse_options"]])) {
    opt_names <- c(opt_names, names(args[["parse_options"]]))
  }

  if ("read_options" %in% names(args) && is.list(args[["read_options"]])) {
    opt_names <- c(opt_names, names(args[["read_options"]]))
  }

  if ("convert_options" %in% names(args) && is.list(args[["convert_options"]])) {
    opt_names <- c(opt_names, names(args[["convert_options"]]))
  }

  setdiff(opt_names, c("parse_options", "read_options", "convert_options"))
}

check_unrecognised_args <- function(opts) {
  # Catch any options with full or partial names that do not match any of the
  # recognized Arrow C++ option names or readr-style option names
  opt_names <- get_opt_names(opts)

  arrow_opts <- c(
    names(formals(CsvParseOptions$create)),
    names(formals(CsvReadOptions$create)),
    names(formals(CsvConvertOptions$create)),
    "schema"
  )

  readr_opts <- c(
    names(formals(readr_to_csv_parse_options)),
    names(formals(readr_to_csv_read_options)),
    "na"
  )

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
}

check_ambiguous_options <- function(passed_opts, opts1, opts2) {
  is_ambig_opt <- is.na(pmatch(passed_opts, c(opts1, opts2)))
  ambig_opts <- passed_opts[is_ambig_opt]
  if (length(ambig_opts)) {
    stop("Ambiguous ",
      ngettext(length(ambig_opts), "option", "options"),
      ": ",
      oxford_paste(ambig_opts),
      ". Use full argument names",
      call. = FALSE
    )
  }
}

check_schema <- function(schema, column_names) {
  if (!is.null(schema) && !inherits(schema, "Schema")) {
    abort(paste0(
      "`schema` must be an object of class 'Schema' not '",
      class(schema)[1],
      "'."
    ))
  }

  schema_names <- names(schema)

  if (!is.null(schema) && !identical(schema_names, column_names)) {
    missing_from_schema <- setdiff(column_names, schema_names)
    missing_from_colnames <- setdiff(schema_names, column_names)
    message_colnames <- NULL
    message_schema <- NULL
    message_order <- NULL

    if (length(missing_from_colnames) > 0) {
      message_colnames <- paste(
        oxford_paste(missing_from_colnames, quote_symbol = "`"),
        "not present in `column_names`"
      )
    }

    if (length(missing_from_schema) > 0) {
      message_schema <- paste(
        oxford_paste(missing_from_schema, quote_symbol = "`"),
        "not present in `schema`"
      )
    }

    if (length(missing_from_schema) == 0 && length(missing_from_colnames) == 0) {
      message_order <- "`column_names` and `schema` field names match but are not in the same order"
    }

    abort(
      c(
        "Values in `column_names` must match `schema` field names",
        x = message_order,
        x = message_schema,
        x = message_colnames
      )
    )
  }
}

# Support both readr-style option names and Arrow C++ option names
csv_file_format_parse_opts <- function(...) {
  opts <- list(...)
  # Filter out arguments meant for CsvConvertOptions/CsvReadOptions
  convert_opts <- c(names(formals(CsvConvertOptions$create)), "na", "convert_options")
  read_opts <- c(
    names(formals(CsvReadOptions$create)),
    names(formals(readr_to_csv_read_options)),
    "read_options"
  )
  opts[convert_opts] <- NULL
  opts[read_opts] <- NULL
  opts[["schema"]] <- NULL
  opts[["parse_options"]] <- NULL
  opt_names <- get_opt_names(opts)

  arrow_opts <- c(names(formals(CsvParseOptions$create)))
  readr_opts <- c(names(formals(readr_to_csv_parse_options)))

  is_arrow_opt <- !is.na(pmatch(opt_names, arrow_opts))
  is_readr_opt <- !is.na(pmatch(opt_names, readr_opts))
  # Catch options with ambiguous partial names (such as "del") that make it
  # unclear whether the user is specifying Arrow C++ options ("delimiter") or
  # readr-style options ("delim")
  check_ambiguous_options(opt_names, arrow_opts, readr_opts)

  if (any(is_readr_opt)) {
    # Catch cases when the user specifies a mix of Arrow C++ options and
    # readr-style options
    if (!all(is_readr_opt)) {
      stop("Use either Arrow parse options or readr parse options, not both",
        call. = FALSE
      )
    }
    do.call(readr_to_csv_parse_options, opts) # all options have readr-style names
  } else {
    do.call(CsvParseOptions$create, opts) # all options have Arrow C++ names
  }
}

csv_file_format_convert_opts <- function(...) {
  opts <- list(...)
  # Filter out arguments meant for CsvParseOptions/CsvReadOptions
  arrow_opts <- c(names(formals(CsvParseOptions$create)), "parse_options")
  readr_opts <- names(formals(readr_to_csv_parse_options))
  read_opts <- c(
    names(formals(CsvReadOptions$create)),
    names(formals(readr_to_csv_read_options)),
    "read_options"
  )
  opts[arrow_opts] <- NULL
  opts[readr_opts] <- NULL
  opts[read_opts] <- NULL
  opts[["schema"]] <- NULL
  opts[["convert_options"]] <- NULL

  # map "na" to "null_values"
  if ("na" %in% names(opts)) {
    opts[["null_values"]] <- opts[["na"]]
    opts[["na"]] <- NULL
  }

  do.call(CsvConvertOptions$create, opts)
}

csv_file_format_read_opts <- function(schema = NULL, ...) {
  opts <- list(...)
  # Filter out arguments meant for CsvParseOptions/CsvConvertOptions
  arrow_opts <- c(names(formals(CsvParseOptions$create)), "parse_options")
  readr_opts <- names(formals(readr_to_csv_parse_options))
  convert_opts <- c(names(formals(CsvConvertOptions$create)), "na", "convert_options")
  opts[arrow_opts] <- NULL
  opts[readr_opts] <- NULL
  opts[convert_opts] <- NULL
  opts[["read_options"]] <- NULL

  opt_names <- names(opts)
  arrow_opts <- c(names(formals(CsvReadOptions$create)))
  readr_opts <- c(names(formals(readr_to_csv_read_options)))

  is_arrow_opt <- !is.na(match(opt_names, arrow_opts))
  is_readr_opt <- !is.na(match(opt_names, readr_opts))

  check_ambiguous_options(opt_names, arrow_opts, readr_opts)

  null_or_true <- function(x) {
    is.null(x) || isTRUE(x)
  }

  if (!is.null(schema) && null_or_true(opts[["column_names"]]) && null_or_true(opts[["col_names"]])) {
    if (any(is_readr_opt)) {
      opts[["col_names"]] <- names(schema)
    } else {
      opts[["column_names"]] <- names(schema)
    }
  }

  if (any(is_readr_opt)) {
    # Catch cases when the user specifies a mix of Arrow C++ options and
    # readr-style options
    if (!all(is_readr_opt)) {
      abort(c(
        "Additional CSV reading options must be Arrow-style or readr-style, but not both.",
        i = sprintf("Arrow options used: %s.", oxford_paste(opt_names[is_arrow_opt])),
        i = sprintf("readr options used: %s.", oxford_paste(opt_names[is_readr_opt]))
      ))
    }
    do.call(readr_to_csv_read_options, opts) # all options have readr-style names
  } else {
    do.call(CsvReadOptions$create, opts) # all options have Arrow C++ names
  }
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
#'   `format = "parquet"`:
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
FragmentScanOptions <- R6Class("FragmentScanOptions",
  inherit = ArrowObject,
  active = list(
    # @description
    # Return the `FragmentScanOptions`'s type
    type = function() dataset___FragmentScanOptions__type_name(self)
  )
)
FragmentScanOptions$create <- function(format, ...) {
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
                                          convert_opts = csv_file_format_convert_opts(...),
                                          read_opts = csv_file_format_read_opts(...)) {
  dataset___CsvFragmentScanOptions__Make(convert_opts, read_opts)
}

#' @usage NULL
#' @format NULL
#' @rdname FragmentScanOptions
#' @export
ParquetFragmentScanOptions <- R6Class("ParquetFragmentScanOptions", inherit = FragmentScanOptions)
ParquetFragmentScanOptions$create <- function(use_buffered_stream = FALSE,
                                              buffer_size = 8196,
                                              pre_buffer = TRUE) {
  dataset___ParquetFragmentScanOptions__Make(use_buffered_stream, buffer_size, pre_buffer)
}

#' Format-specific write options
#'
#' @description
#' A `FileWriteOptions` holds write options specific to a `FileFormat`.
FileWriteOptions <- R6Class("FileWriteOptions",
  inherit = ArrowObject,
  public = list(
    update = function(column_names, ...) {
      check_additional_args <- function(format, passed_args) {
        if (format == "parquet") {
          supported_args <- names(formals(write_parquet))
          supported_args <- supported_args[supported_args != c("x", "sink")]
        } else if (format == "ipc") {
          supported_args <- c(
            "use_legacy_format",
            "metadata_version",
            "codec",
            "null_fallback"
          )
        } else if (format == "csv") {
          supported_args <- c(
            names(formals(CsvWriteOptions$create)),
            names(formals(readr_to_csv_write_options))
          )
        }

        unsupported_passed_args <- setdiff(passed_args, supported_args)

        if (length(unsupported_passed_args) > 0) {
          err_header <- paste0(
            oxford_paste(unsupported_passed_args, quote_symbol = "`"),
            ngettext(
              length(unsupported_passed_args),
              " is not a valid argument ",
              " are not valid arguments "
            ),
            "for your chosen `format`."
          )
          err_info <- NULL
          arg_info <- paste0(
            "Supported arguments: ",
            oxford_paste(unique(supported_args), quote_symbol = "`"),
            "."
          )
          if ("compression" %in% unsupported_passed_args) {
            err_info <- "You could try using `codec` instead of `compression`."
          }
          abort(c(err_header, i = err_info, i = arg_info))
        }
      }

      args <- list(...)
      check_additional_args(self$type, names(args))

      if (self$type == "parquet") {
        dataset___ParquetFileWriteOptions__update(
          self,
          ParquetWriterProperties$create(column_names, ...),
          ParquetArrowWriterProperties$create(...)
        )
      } else if (self$type == "ipc") {
        if (is.null(args$codec)) {
          dataset___IpcFileWriteOptions__update1(
            self,
            get_ipc_use_legacy_format(args$use_legacy_format),
            get_ipc_metadata_version(args$metadata_version)
          )
        } else {
          dataset___IpcFileWriteOptions__update2(
            self,
            get_ipc_use_legacy_format(args$use_legacy_format),
            args$codec,
            get_ipc_metadata_version(args$metadata_version)
          )
        }
      } else if (self$type == "csv") {
        arrow_opts <- names(formals(CsvWriteOptions$create))
        readr_opts <- names(formals(readr_to_csv_write_options))
        readr_only_opts <- setdiff(readr_opts, arrow_opts)

        is_arrow_opt <- !is.na(pmatch(names(args), arrow_opts))
        is_readr_opt <- !is.na(pmatch(names(args), readr_opts))
        is_readr_only_opt <- !is.na(pmatch(names(args), readr_only_opts))

        # These option names aren't mutually exclusive, so only use readr path
        # if we have at least one readr-specific option.
        if (sum(is_readr_only_opt)) {
          dataset___CsvFileWriteOptions__update(
            self,
            do.call(readr_to_csv_write_options, args[is_readr_opt])
          )
        } else {
          dataset___CsvFileWriteOptions__update(
            self,
            do.call(CsvWriteOptions$create, args[is_arrow_opt])
          )
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
