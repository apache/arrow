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

#' Read a Parquet file
#'
#' '[Parquet](https://parquet.apache.org/)' is a columnar storage file format.
#' This function enables you to read Parquet files into R.
#'
#' @inheritParams read_feather
#' @param props [ParquetArrowReaderProperties]
#' @param ... Additional arguments passed to `ParquetFileReader$create()`
#'
#' @return A [arrow::Table][Table], or a `data.frame` if `as_data_frame` is
#' `TRUE` (the default).
#' @examplesIf arrow_with_parquet() && !getFromNamespace("on_linux_dev", "arrow")()
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' write_parquet(mtcars, tf)
#' df <- read_parquet(tf, col_select = starts_with("d"))
#' head(df)
#' @export
read_parquet <- function(file,
                         col_select = NULL,
                         as_data_frame = TRUE,
                         # TODO: for consistency with other readers/writers,
                         # these properties should be enumerated as args here,
                         # and ParquetArrowReaderProperties$create() should
                         # accept them, as with ParquetWriterProperties.
                         # Assembling `props` yourself is something you do with
                         # ParquetFileReader but not here.
                         props = ParquetArrowReaderProperties$create(),
                         ...) {
  if (!inherits(file, "RandomAccessFile")) {
    # Compression is handled inside the parquet file format, so we don't need
    # to detect from the file extension and wrap in a CompressedInputStream
    file <- make_readable_file(file)
    on.exit(file$close())
  }
  reader <- ParquetFileReader$create(file, props = props, ...)

  col_select <- enquo(col_select)
  if (!quo_is_null(col_select)) {
    # infer which columns to keep from schema
    sim_df <- as.data.frame(reader$GetSchema())
    indices <- eval_select(col_select, sim_df) - 1L
    tab <- tryCatch(
      reader$ReadTable(indices),
      error = read_compressed_error
    )
  } else {
    # read all columns
    tab <- tryCatch(
      reader$ReadTable(),
      error = read_compressed_error
    )
  }

  if (as_data_frame) {
    tab <- as.data.frame(tab)
  }
  tab
}

#' Write Parquet file to disk
#'
#' [Parquet](https://parquet.apache.org/) is a columnar storage file format.
#' This function enables you to write Parquet files from R.
#'
#' Due to features of the format, Parquet files cannot be appended to.
#' If you want to use the Parquet format but also want the ability to extend
#' your dataset, you can write to additional Parquet files and then treat
#' the whole directory of files as a [Dataset] you can query.
#' See the \href{https://arrow.apache.org/docs/r/articles/dataset.html}{dataset
#' article} for examples of this.
#'
#' @param x `data.frame`, [RecordBatch], or [Table]
#' @param sink A string file path, URI, or [OutputStream], or path in a file
#' system (`SubTreeFileSystem`)
#' @param chunk_size how many rows of data to write to disk at once. This
#'    directly corresponds to how many rows will be in each row group in
#'    parquet. If `NULL`, a best guess will be made for optimal size (based on
#'    the number of columns and number of rows), though if the data has fewer
#'    than 250 million cells (rows x cols), then the total number of rows is
#'    used.
#' @param version parquet version: "1.0", "2.0" (deprecated), "2.4" (default),
#'    "2.6", or "latest" (currently equivalent to 2.6). Numeric values are
#'    coerced to character.
#' @param compression compression algorithm. Default "snappy". See details.
#' @param compression_level compression level. Meaning depends on compression
#'    algorithm
#' @param use_dictionary logical: use dictionary encoding? Default `TRUE`
#' @param write_statistics logical: include statistics? Default `TRUE`
#' @param data_page_size Set a target threshold for the approximate encoded
#'    size of data pages within a column chunk (in bytes). Default 1 MiB.
#' @param use_deprecated_int96_timestamps logical: write timestamps to INT96
#'    Parquet format, which has been deprecated? Default `FALSE`.
#' @param coerce_timestamps Cast timestamps a particular resolution. Can be
#'   `NULL`, "ms" or "us". Default `NULL` (no casting)
#' @param allow_truncated_timestamps logical: Allow loss of data when coercing
#'    timestamps to a particular resolution. E.g. if microsecond or nanosecond
#'    data is lost when coercing to "ms", do not raise an exception. Default
#'    `FALSE`.
#'
#' @details The parameters `compression`, `compression_level`, `use_dictionary` and
#'   `write_statistics` support various patterns:
#'
#'  - The default `NULL` leaves the parameter unspecified, and the C++ library
#'    uses an appropriate default for each column (defaults listed above)
#'  - A single, unnamed, value (e.g. a single string for `compression`) applies to all columns
#'  - An unnamed vector, of the same size as the number of columns, to specify a
#'    value for each column, in positional order
#'  - A named vector, to specify the value for the named columns, the default
#'    value for the setting is used when not supplied
#'
#' The `compression` argument can be any of the following (case insensitive):
#' "uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4", "lzo" or "bz2".
#' Only "uncompressed" is guaranteed to be available, but "snappy" and "gzip"
#' are almost always included. See [codec_is_available()].
#' The default "snappy" is used if available, otherwise "uncompressed". To
#' disable compression, set `compression = "uncompressed"`.
#' Note that "uncompressed" columns may still have dictionary encoding.
#'
#' @return the input `x` invisibly.
#' @seealso [ParquetFileWriter] for a lower-level interface to Parquet writing.
#' @examplesIf arrow_with_parquet()
#' tf1 <- tempfile(fileext = ".parquet")
#' write_parquet(data.frame(x = 1:5), tf1)
#'
#' # using compression
#' if (codec_is_available("gzip")) {
#'   tf2 <- tempfile(fileext = ".gz.parquet")
#'   write_parquet(data.frame(x = 1:5), tf2, compression = "gzip", compression_level = 5)
#' }
#' @export
write_parquet <- function(x,
                          sink,
                          chunk_size = NULL,
                          # writer properties
                          version = "2.4",
                          compression = default_parquet_compression(),
                          compression_level = NULL,
                          use_dictionary = NULL,
                          write_statistics = NULL,
                          data_page_size = NULL,
                          # arrow writer properties
                          use_deprecated_int96_timestamps = FALSE,
                          coerce_timestamps = NULL,
                          allow_truncated_timestamps = FALSE) {
  x_out <- x
  x <- as_writable_table(x)

  if (!inherits(sink, "OutputStream")) {
    # TODO(ARROW-17221): if (missing(compression)), we could detect_compression(sink) here
    sink <- make_output_stream(sink)
    on.exit(sink$close())
  }

  writer <- ParquetFileWriter$create(
    x$schema,
    sink,
    properties = ParquetWriterProperties$create(
      names(x),
      version = version,
      compression = compression,
      compression_level = compression_level,
      use_dictionary = use_dictionary,
      write_statistics = write_statistics,
      data_page_size = data_page_size
    ),
    arrow_properties = ParquetArrowWriterProperties$create(
      use_deprecated_int96_timestamps = use_deprecated_int96_timestamps,
      coerce_timestamps = coerce_timestamps,
      allow_truncated_timestamps = allow_truncated_timestamps
    )
  )

  # determine an approximate chunk size
  if (is.null(chunk_size)) {
    chunk_size <- calculate_chunk_size(x$num_rows, x$num_columns)
  }

  writer$WriteTable(x, chunk_size = chunk_size)
  writer$Close()

  invisible(x_out)
}

default_parquet_compression <- function() {
  # Match the pyarrow default (overriding the C++ default)
  if (codec_is_available("snappy")) {
    "snappy"
  } else {
    NULL
  }
}

ParquetArrowWriterProperties <- R6Class("ParquetArrowWriterProperties", inherit = ArrowObject)
ParquetArrowWriterProperties$create <- function(use_deprecated_int96_timestamps = FALSE,
                                                coerce_timestamps = NULL,
                                                allow_truncated_timestamps = FALSE,
                                                ...) {
  if (is.null(coerce_timestamps)) {
    timestamp_unit <- -1L # null sentinel value
  } else {
    timestamp_unit <- make_valid_time_unit(
      coerce_timestamps,
      c("ms" = TimeUnit$MILLI, "us" = TimeUnit$MICRO)
    )
  }
  parquet___ArrowWriterProperties___create(
    use_deprecated_int96_timestamps = isTRUE(use_deprecated_int96_timestamps),
    timestamp_unit = timestamp_unit,
    allow_truncated_timestamps = isTRUE(allow_truncated_timestamps)
  )
}

valid_parquet_version <- c(
  "1.0" = ParquetVersionType$PARQUET_1_0,
  "2.0" = ParquetVersionType$PARQUET_2_0,
  "2.4" = ParquetVersionType$PARQUET_2_4,
  "2.6" = ParquetVersionType$PARQUET_2_6,
  "latest" = ParquetVersionType$PARQUET_2_6
)

make_valid_parquet_version <- function(version, valid_versions = valid_parquet_version) {
  if (is_integerish(version)) {
    version <- as.numeric(version)
  }
  if (is.numeric(version)) {
    version <- format(version, nsmall = 1)
  }

  if (!is.string(version)) {
    stop(
      "`version` must be one of ", oxford_paste(names(valid_versions), "or"),
      call. = FALSE
    )
  }
  out <- valid_versions[[arg_match(version, values = names(valid_versions))]]

  if (identical(out, ParquetVersionType$PARQUET_2_0)) {
    warning(
      'Parquet format version "2.0" is deprecated. Use "2.4" or "2.6" to select format features.',
      call. = FALSE
    )
  }
  out
}

#' @title ParquetWriterProperties class
#' @rdname ParquetWriterProperties
#' @name ParquetWriterProperties
#' @docType class
#' @usage NULL
#' @format NULL
#' @description This class holds settings to control how a Parquet file is read
#' by [ParquetFileWriter].
#'
#' @section Factory:
#'
#' The `ParquetWriterProperties$create()` factory method instantiates the object
#' and takes the following arguments:
#'
#' - `table`: table to write (required)
#' - `version`: Parquet version, "1.0" or "2.0". Default "1.0"
#' - `compression`: Compression type, algorithm `"uncompressed"`
#' - `compression_level`: Compression level; meaning depends on compression algorithm
#' - `use_dictionary`: Specify if we should use dictionary encoding. Default `TRUE`
#' - `write_statistics`: Specify if we should write statistics. Default `TRUE`
#' - `data_page_size`: Set a target threshold for the approximate encoded
#'    size of data pages within a column chunk (in bytes). Default 1 MiB.
#'
#' @details The parameters `compression`, `compression_level`, `use_dictionary`
#'   and write_statistics` support various patterns:
#'
#'  - The default `NULL` leaves the parameter unspecified, and the C++ library
#'    uses an appropriate default for each column (defaults listed above)
#'  - A single, unnamed, value (e.g. a single string for `compression`) applies to all columns
#'  - An unnamed vector, of the same size as the number of columns, to specify a
#'    value for each column, in positional order
#'  - A named vector, to specify the value for the named columns, the default
#'    value for the setting is used when not supplied
#'
#' Unlike the high-level [write_parquet], `ParquetWriterProperties` arguments
#' use the C++ defaults. Currently this means "uncompressed" rather than
#' "snappy" for the `compression` argument.
#'
#' @seealso [write_parquet]
#' @seealso [Schema] for information about schemas and metadata handling.
#'
#' @export
ParquetWriterProperties <- R6Class("ParquetWriterProperties", inherit = ArrowObject)
ParquetWriterPropertiesBuilder <- R6Class("ParquetWriterPropertiesBuilder",
  inherit = ArrowObject,
  public = list(
    set_version = function(version) {
      parquet___WriterProperties___Builder__version(self, make_valid_parquet_version(version))
    },
    set_compression = function(column_names, compression) {
      compression <- compression_from_name(compression)
      assert_that(is.integer(compression))
      private$.set(
        column_names, compression,
        parquet___ArrowWriterProperties___Builder__set_compressions
      )
    },
    set_compression_level = function(column_names, compression_level) {
      # cast to integer but keep names
      compression_level <- set_names(as.integer(compression_level), names(compression_level))
      private$.set(
        column_names, compression_level,
        parquet___ArrowWriterProperties___Builder__set_compression_levels
      )
    },
    set_dictionary = function(column_names, use_dictionary) {
      assert_that(is.logical(use_dictionary))
      private$.set(
        column_names, use_dictionary,
        parquet___ArrowWriterProperties___Builder__set_use_dictionary
      )
    },
    set_write_statistics = function(column_names, write_statistics) {
      assert_that(is.logical(write_statistics))
      private$.set(
        column_names, write_statistics,
        parquet___ArrowWriterProperties___Builder__set_write_statistics
      )
    },
    set_data_page_size = function(data_page_size) {
      parquet___ArrowWriterProperties___Builder__data_page_size(self, data_page_size)
    }
  ),
  private = list(
    .set = function(column_names, value, FUN) {
      msg <- paste0("unsupported ", substitute(value), "= specification")
      given_names <- names(value)
      if (is.null(given_names)) {
        if (length(value) %in% c(1L, length(column_names))) {
          # If there's a single, unnamed value, FUN will set it globally
          # If there are values for all columns, send them along with the names
          FUN(self, column_names, value)
        } else {
          abort(msg)
        }
      } else if (all(given_names %in% column_names)) {
        # Use the given names
        FUN(self, given_names, value)
      } else {
        abort(msg)
      }
    }
  )
)

ParquetWriterProperties$create <- function(column_names,
                                           version = NULL,
                                           compression = default_parquet_compression(),
                                           compression_level = NULL,
                                           use_dictionary = NULL,
                                           write_statistics = NULL,
                                           data_page_size = NULL,
                                           ...) {
  builder <- parquet___WriterProperties___Builder__create()
  if (!is.null(version)) {
    builder$set_version(version)
  }
  if (!is.null(compression)) {
    builder$set_compression(column_names, compression = compression)
  }
  if (!is.null(compression_level)) {
    builder$set_compression_level(column_names, compression_level = compression_level)
  }
  if (!is.null(use_dictionary)) {
    builder$set_dictionary(column_names, use_dictionary)
  }
  if (!is.null(write_statistics)) {
    builder$set_write_statistics(column_names, write_statistics)
  }
  if (!is.null(data_page_size)) {
    builder$set_data_page_size(data_page_size)
  }
  parquet___WriterProperties___Builder__build(builder)
}

#' @title ParquetFileWriter class
#' @rdname ParquetFileWriter
#' @name ParquetFileWriter
#' @docType class
#' @usage NULL
#' @format NULL
#' @description This class enables you to interact with Parquet files.
#'
#' @section Factory:
#'
#' The `ParquetFileWriter$create()` factory method instantiates the object and
#' takes the following arguments:
#'
#' - `schema` A [Schema]
#' - `sink` An [arrow::io::OutputStream][OutputStream]
#' - `properties` An instance of [ParquetWriterProperties]
#' - `arrow_properties` An instance of `ParquetArrowWriterProperties`
#'
#' @section Methods:
#'
#' - `WriteTable` Write a [Table] to `sink`
#' - `Close` Close the writer. Note: does not close the `sink`.
#'   [arrow::io::OutputStream][OutputStream] has its own `close()` method.
#'
#' @export
#' @include arrow-object.R
ParquetFileWriter <- R6Class("ParquetFileWriter",
  inherit = ArrowObject,
  public = list(
    WriteTable = function(table, chunk_size) {
      parquet___arrow___FileWriter__WriteTable(self, table, chunk_size)
    },
    Close = function() parquet___arrow___FileWriter__Close(self)
  )
)
ParquetFileWriter$create <- function(schema,
                                     sink,
                                     properties = ParquetWriterProperties$create(),
                                     arrow_properties = ParquetArrowWriterProperties$create()) {
  assert_is(sink, "OutputStream")
  parquet___arrow___ParquetFileWriter__Open(schema, sink, properties, arrow_properties)
}


#' @title ParquetFileReader class
#' @rdname ParquetFileReader
#' @name ParquetFileReader
#' @docType class
#' @usage NULL
#' @format NULL
#' @description This class enables you to interact with Parquet files.
#'
#' @section Factory:
#'
#' The `ParquetFileReader$create()` factory method instantiates the object and
#' takes the following arguments:
#'
#' - `file` A character file name, raw vector, or Arrow file connection object
#'    (e.g. `RandomAccessFile`).
#' - `props` Optional [ParquetArrowReaderProperties]
#' - `mmap` Logical: whether to memory-map the file (default `TRUE`)
#' - `...` Additional arguments, currently ignored
#'
#' @section Methods:
#'
#' - `$ReadTable(column_indices)`: get an `arrow::Table` from the file. The optional
#'    `column_indices=` argument is a 0-based integer vector indicating which columns to retain.
#' - `$ReadRowGroup(i, column_indices)`: get an `arrow::Table` by reading the `i`th row group (0-based).
#'    The optional `column_indices=` argument is a 0-based integer vector indicating which columns to retain.
#' - `$ReadRowGroups(row_groups, column_indices)`: get an `arrow::Table` by reading several row
#'    groups (0-based integers).
#'    The optional `column_indices=` argument is a 0-based integer vector indicating which columns to retain.
#' - `$GetSchema()`: get the `arrow::Schema` of the data in the file
#' - `$ReadColumn(i)`: read the `i`th column (0-based) as a [ChunkedArray].
#'
#' @section Active bindings:
#'
#' - `$num_rows`: number of rows.
#' - `$num_columns`: number of columns.
#' - `$num_row_groups`: number of row groups.
#'
#' @export
#' @examplesIf arrow_with_parquet()
#' f <- system.file("v0.7.1.parquet", package = "arrow")
#' pq <- ParquetFileReader$create(f)
#' pq$GetSchema()
#' if (codec_is_available("snappy")) {
#'   # This file has compressed data columns
#'   tab <- pq$ReadTable()
#'   tab$schema
#' }
#' @include arrow-object.R
ParquetFileReader <- R6Class("ParquetFileReader",
  inherit = ArrowObject,
  active = list(
    num_rows = function() {
      as.integer(parquet___arrow___FileReader__num_rows(self))
    },
    num_columns = function() {
      parquet___arrow___FileReader__num_columns(self)
    },
    num_row_groups = function() {
      parquet___arrow___FileReader__num_row_groups(self)
    }
  ),
  public = list(
    ReadTable = function(column_indices = NULL) {
      if (is.null(column_indices)) {
        parquet___arrow___FileReader__ReadTable1(self)
      } else {
        column_indices <- vec_cast(column_indices, integer())
        parquet___arrow___FileReader__ReadTable2(self, column_indices)
      }
    },
    ReadRowGroup = function(i, column_indices = NULL) {
      i <- vec_cast(i, integer())
      if (is.null(column_indices)) {
        parquet___arrow___FileReader__ReadRowGroup1(self, i)
      } else {
        column_indices <- vec_cast(column_indices, integer())
        parquet___arrow___FileReader__ReadRowGroup2(self, i, column_indices)
      }
    },
    ReadRowGroups = function(row_groups, column_indices = NULL) {
      row_groups <- vec_cast(row_groups, integer())
      if (is.null(column_indices)) {
        parquet___arrow___FileReader__ReadRowGroups1(self, row_groups)
      } else {
        column_indices <- vec_cast(column_indices, integer())
        parquet___arrow___FileReader__ReadRowGroups2(self, row_groups, column_indices)
      }
    },
    ReadColumn = function(i) {
      i <- vec_cast(i, integer())
      parquet___arrow___FileReader__ReadColumn(self, i)
    },
    GetSchema = function() {
      parquet___arrow___FileReader__GetSchema(self)
    }
  )
)

ParquetFileReader$create <- function(file,
                                     props = ParquetArrowReaderProperties$create(),
                                     mmap = TRUE,
                                     ...) {
  file <- make_readable_file(file, mmap)
  assert_is(props, "ParquetArrowReaderProperties")
  assert_is(file, "RandomAccessFile")

  parquet___arrow___FileReader__OpenFile(file, props)
}

#' @title ParquetArrowReaderProperties class
#' @rdname ParquetArrowReaderProperties
#' @name ParquetArrowReaderProperties
#' @docType class
#' @usage NULL
#' @format NULL
#' @description This class holds settings to control how a Parquet file is read
#' by [ParquetFileReader].
#'
#' @section Factory:
#'
#' The `ParquetArrowReaderProperties$create()` factory method instantiates the object
#' and takes the following arguments:
#'
#' - `use_threads` Logical: whether to use multithreading (default `TRUE`)
#'
#' @section Methods:
#'
#' - `$read_dictionary(column_index)`
#' - `$set_read_dictionary(column_index, read_dict)`
#' - `$use_threads(use_threads)`
#'
#' @export
ParquetArrowReaderProperties <- R6Class("ParquetArrowReaderProperties",
  inherit = ArrowObject,
  public = list(
    read_dictionary = function(column_index) {
      parquet___arrow___ArrowReaderProperties__get_read_dictionary(self, column_index)
    },
    set_read_dictionary = function(column_index, read_dict) {
      parquet___arrow___ArrowReaderProperties__set_read_dictionary(self, column_index, read_dict)
    },
    coerce_int96_timestamp_unit = function() {
      parquet___arrow___ArrowReaderProperties__get_coerce_int96_timestamp_unit(self)
    },
    set_coerce_int96_timestamp_unit = function(unit) {
      parquet___arrow___ArrowReaderProperties__set_coerce_int96_timestamp_unit(self, unit)
    }
  ),
  active = list(
    use_threads = function(use_threads) {
      if (missing(use_threads)) {
        parquet___arrow___ArrowReaderProperties__get_use_threads(self)
      } else {
        parquet___arrow___ArrowReaderProperties__set_use_threads(self, use_threads)
      }
    }
  )
)

ParquetArrowReaderProperties$create <- function(use_threads = option_use_threads()) {
  parquet___arrow___ArrowReaderProperties__Make(isTRUE(use_threads))
}

calculate_chunk_size <- function(rows, columns,
                                 target_cells_per_group = getOption("arrow.parquet_cells_per_group", 2.5e8),
                                 max_chunks = getOption("arrow.parquet_max_chunks", 200)) {

  # Ensure is a float to prevent integer overflow issues
  num_cells <- as.numeric(rows) * as.numeric(columns)

  if (num_cells < target_cells_per_group) {
    # If the total number of cells is less than the default 250 million, we want one group
    num_chunks <- 1
  } else {
    # no more than the default 250 million cells (rows * cols) per group
    # and we use floor, then ceiling to ensure that these are whole numbers
    num_chunks <- floor(num_cells / target_cells_per_group)
  }

  # but there are no more than 200 chunks
  num_chunks <- min(num_chunks, max_chunks)

  chunk_size <- ceiling(rows / num_chunks)

  chunk_size
}
