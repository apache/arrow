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

ParquetArrowWriterProperties_Builder <- R6Class("ParquetArrowWriterProperties_Builder", inherit = Object,
  public = list(
    store_schema = function() {
      parquet___ArrowWriterProperties___Builder__store_schema(self)
      self
    },
    set_int96_support = function(use_deprecated_int96_timestamps = FALSE) {
      if (use_deprecated_int96_timestamps) {
        parquet___ArrowWriterProperties___Builder__enable_deprecated_int96_timestamps(self)
      } else {
        parquet___ArrowWriterProperties___Builder__disable_deprecated_int96_timestamps(self)
      }
      self
    },
    set_coerce_timestamps = function(coerce_timestamps = NULL) {
      if (!is.null(coerce_timestamps)) {
        if (coerce_timestamps == "ms") {
          parquet___ArrowWriterProperties___Builder__coerce_timestamps(TimeUnit$MILLI)
        } else if (coerce_timestamps == "us") {
          parquet___ArrowWriterProperties___Builder__coerce_timestamps(TimeUnit$MICRO)
        } else {
          abort("Invalid value for coerce_timestamps")
        }
      }
      self
    },
    set_allow_truncated_timestamps = function(allow_truncated_timestamps = FALSE) {
      if (allow_truncated_timestamps) {
        parquet___ArrowWriterProperties___Builder__allow_truncated_timestamps(self)
      } else {
        parquet___ArrowWriterProperties___Builder__disallow_truncated_timestamps(self)
      }

      self
    }

  )
)
ParquetArrowWriterProperties <- R6Class("ParquetArrowWriterProperties", inherit = Object)

ParquetArrowWriterProperties$default <- function() {
  shared_ptr(ParquetArrowWriterProperties, parquet___default_arrow_writer_properties())
}

ParquetArrowWriterProperties$create <- function(use_deprecated_int96_timestamps = FALSE, coerce_timestamps = NULL, allow_truncated_timestamps = FALSE) {
  builder <- shared_ptr(ParquetArrowWriterProperties_Builder, parquet___ArrowWriterProperties___Builder__create())
  builder$store_schema()
  builder$set_int96_support(use_deprecated_int96_timestamps)
  builder$set_coerce_timestamps(coerce_timestamps)
  builder$set_allow_truncated_timestamps(allow_truncated_timestamps)
  shared_ptr(ParquetArrowWriterProperties, parquet___ArrowWriterProperties___Builder__build(builder))
}

ParquetWriterProperties <- R6Class("ParquetWriterProperties", inherit = Object)
ParquetWriterProperties$default <- function() {
  shared_ptr(ParquetWriterProperties, parquet___default_writer_properties())
}

ParquetWriterProperties_Builder <- R6Class("ParquetWriterProperties_Builder", inherit = Object,
  public = list(
    set_version = function(version = NULL) {
      if (!is.null(version)) {
        if (identical(version, "1.0")) {
          parquet___ArrowWriterProperties___Builder__version(self, ParquetVersionType$PARQUET_1_0)
        } else if (identical(version, "2.0")) {
          parquet___ArrowWriterProperties___Builder__version(self, ParquetVersionType$PARQUET_2_0)
        } else {
          abort("unknown parquet version")
        }
      }
    },

    set_compression = function(compression){
      if (is.character(compression) && length(compression) == 1L) {
        type <- CompressionType[[match.arg(toupper(compression), names(CompressionType))]]
        parquet___ArrowWriterProperties___Builder__default_compression(self, type)
      } else if(inherits(compression, "Codec")) {
        # TODO: Codec does not give a way to access its compression level, e.g. compression$level
        parquet___ArrowWriterProperties___Builder__default_compression(self, compression$name)
      } else {
        abort("compression specification not supported yet")
      }
    },

    set_compression_level = function(compression_level){
      if (rlang::is_integerish(compression_level) && length(compression_level) == 1L) {
        parquet___ArrowWriterProperties___Builder__default_compression_level(self, compression_level)
      } else {
        abort("compression_level specification not supported yet")
      }
    },

    set_dictionary = function(use_dictionary) {
      if (is.logical(use_dictionary) && length(use_dictionary) == 1L) {
        parquet___ArrowWriterProperties___Builder__default_use_dictionary(self, isTRUE(use_dictionary))
      } else {
        abort("use_dictionary specification not supported yet")
      }
    },

    set_write_statistics = function(write_statistics) {
      if (is.logical(write_statistics) && length(write_statistics) == 1L) {
        parquet___ArrowWriterProperties___Builder__default_write_statistics(self, isTRUE(write_statistics))
      } else {
        abort("write_statistics specification not supported yet")
      }
    },

    set_data_page_size = function(data_page_size) {
      parquet___ArrowWriterProperties___Builder__data_page_size(self, data_page_size)
    }
  )
)

ParquetWriterProperties$create <- function(version = NULL, compression = NULL, compression_level = NULL, use_dictionary = NULL, write_statistics = NULL, data_page_size = NULL) {
  if (is.null(version) && is.null(compression) && is.null(compression_level) && is.null(use_dictionary) && is.null(write_statistics) && is.null(data_page_size)) {
    ParquetWriterProperties$default()
  } else {
    builder <- shared_ptr(ParquetWriterProperties_Builder, parquet___WriterProperties___Builder__create())
    builder$set_version(version)
    if (!is.null(compression)) {
      builder$set_compression(compression)
    }
    if (!is.null(compression_level)) {
      builder$set_compression_level(compression_level)
    }
    if (!is.null(use_dictionary)) {
      builder$set_dictionary(use_dictionary)
    }
    if (!is.null(write_statistics)) {
      builder$set_write_statistics(write_statistics)
    }
    if (!is.null(data_page_size)) {
      builder$set_data_page_size(data_page_size)
    }
    shared_ptr(ParquetWriterProperties, parquet___WriterProperties___Builder__build(builder))
  }
}

ParquetFileWriter <- R6Class("ParquetFileWriter", inherit = Object,
  public = list(
    WriteTable = function(table, chunk_size) {
      parquet___arrow___FileWriter__WriteTable(self, table, chunk_size)
    },
    Close = function() {
      parquet___arrow___FileWriter__Close(self)
    }
  )

)
ParquetFileWriter$create <- function(
  schema,
  sink,
  properties = ParquetWriterProperties$default(),
  arrow_properties = ParquetArrowWriterProperties$default()
) {
  unique_ptr(
    ParquetFileWriter,
    parquet___arrow___ParquetFileWriter__Open(schema, sink, properties, arrow_properties)
  )
}

#' Write Parquet file to disk
#'
#' [Parquet](https://parquet.apache.org/) is a columnar storage file format.
#' This function enables you to write Parquet files from R.
#'
#' @param table An [arrow::Table][Table], or an object convertible to it with [to_arrow()]
#' @param sink an [arrow::io::OutputStream][OutputStream] or a string which is interpreted as a file path
#' @param chunk_size chunk size. If NULL, the number of rows of the table is used
#'
#' @param version parquet version
#' @param compression compression name
#' @param compression_level compression level
#' @param use_dictionary Specify if we should use dictionary encoding
#' @param write_statistics Specify if we should write statistics
#' @param data_page_size Set a target threshhold for the approximate encoded size of data
#'        pages within a column chunk. If None, use the default data page size (1Mb) is used.
#' @param properties properties for parquet writer, derived from arguments `version`, `compression`, `use_dictionary`, `write_statistics` and `data_page_size`
#'
#' @param use_deprecated_int96_timestamps Write timestamps to INT96 Parquet format
#' @param coerce_timestamps Cast timestamps a particular resolution. can be NULL, "ms" or "us"
#' @param allow_truncated_timestamps Allow loss of data when coercing timestamps to a particular
#'    resolution. E.g. if microsecond or nanosecond data is lost when coercing to
#'    ms', do not raise an exception
#'
#' @param arrow_properties arrow specific writer properties, derived from arguments `use_deprecated_int96_timestamps`, `coerce_timestamps` and `allow_truncated_timestamps`
#'
#' @examples
#' \donttest{
#' tf1 <- tempfile(fileext = ".parquet")
#' write_parquet(tibble::tibble(x = 1:5), tf2)
#'
#' # using compression
#' tf2 <- tempfile(fileext = ".gz.parquet")
#' write_parquet(tibble::tibble(x = 1:5), CompressedOutputStream$create(tf2, "gzip"))
#'
#' }
#' @export
write_parquet <- function(
  table,
  sink, chunk_size = NULL,
  version = NULL, compression = NULL, compression_level = NULL, use_dictionary = NULL, write_statistics = NULL, data_page_size = NULL,
  properties = ParquetWriterProperties$create(
    version = version,
    compression = compression,
    compression_level = compression_level,
    use_dictionary = use_dictionary,
    write_statistics = write_statistics,
    data_page_size = data_page_size
  ),

  use_deprecated_int96_timestamps = FALSE, coerce_timestamps = NULL, allow_truncated_timestamps = FALSE,
  arrow_properties = ParquetArrowWriterProperties$create(
    use_deprecated_int96_timestamps = use_deprecated_int96_timestamps,
    coerce_timestamps = coerce_timestamps,
    allow_truncated_timestamps = allow_truncated_timestamps
  )
) {
  table <- to_arrow(table)

  if (is.character(sink)) {
    sink <- FileOutputStream$create(sink)
    on.exit(sink$close())
  } else if (!inherits(sink, OutputStream)) {
    abort("sink must be a file path or an OutputStream")
  }

  schema <- table$schema
  writer <- ParquetFileWriter$create(schema, sink, properties = properties, arrow_properties = arrow_properties)
  writer$WriteTable(table, chunk_size = chunk_size %||% table$num_rows)
  writer$Close()
}

#' Read a Parquet file
#'
#' '[Parquet](https://parquet.apache.org/)' is a columnar storage file format.
#' This function enables you to read Parquet files into R.
#'
#' @inheritParams read_delim_arrow
#' @param props [ParquetReaderProperties]
#' @param ... Additional arguments passed to `ParquetFileReader$create()`
#'
#' @return A [arrow::Table][Table], or a `data.frame` if `as_data_frame` is
#' `TRUE`.
#' @examples
#' \donttest{
#' df <- read_parquet(system.file("v0.7.1.parquet", package="arrow"))
#' head(df)
#' }
#' @export
read_parquet <- function(file,
                         col_select = NULL,
                         as_data_frame = TRUE,
                         props = ParquetReaderProperties$create(),
                         ...) {
  reader <- ParquetFileReader$create(file, props = props, ...)
  tab <- reader$ReadTable(!!enquo(col_select))

  if (as_data_frame) {
    tab <- as.data.frame(tab)
  }
  tab
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
#' - `props` Optional [ParquetReaderProperties]
#' - `mmap` Logical: whether to memory-map the file (default `TRUE`)
#' - `...` Additional arguments, currently ignored
#'
#' @section Methods:
#'
#' - `$ReadTable(col_select)`: get an `arrow::Table` from the file, possibly
#'    with columns filtered by a character vector of column names or a
#'    `tidyselect` specification.
#' - `$GetSchema()`: get the `arrow::Schema` of the data in the file
#'
#' @export
#' @examples
#' \donttest{
#' f <- system.file("v0.7.1.parquet", package="arrow")
#' pq <- ParquetFileReader$create(f)
#' pq$GetSchema()
#' tab <- pq$ReadTable(starts_with("c"))
#' tab$schema
#' }
#' @include arrow-package.R
ParquetFileReader <- R6Class("ParquetFileReader",
  inherit = Object,
  public = list(
    ReadTable = function(col_select = NULL) {
      col_select <- enquo(col_select)
      if (quo_is_null(col_select)) {
        shared_ptr(Table, parquet___arrow___FileReader__ReadTable1(self))
      } else {
        all_vars <- shared_ptr(Schema, parquet___arrow___FileReader__GetSchema(self))$names
        indices <- match(vars_select(all_vars, !!col_select), all_vars) - 1L
        shared_ptr(Table, parquet___arrow___FileReader__ReadTable2(self, indices))
      }
    },
    GetSchema = function() {
      shared_ptr(Schema, parquet___arrow___FileReader__GetSchema(self))
    }
  )
)

ParquetFileReader$create <- function(file,
                                     props = ParquetReaderProperties$create(),
                                     mmap = TRUE,
                                     ...) {
  file <- make_readable_file(file, mmap)
  assert_is(props, "ParquetReaderProperties")

  unique_ptr(ParquetFileReader, parquet___arrow___FileReader__OpenFile(file, props))
}

#' @title ParquetReaderProperties class
#' @rdname ParquetReaderProperties
#' @name ParquetReaderProperties
#' @docType class
#' @usage NULL
#' @format NULL
#' @description This class holds settings to control how a Parquet file is read
#' by [ParquetFileReader].
#'
#' @section Factory:
#'
#' The `ParquetReaderProperties$create()` factory method instantiates the object
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
ParquetReaderProperties <- R6Class("ParquetReaderProperties",
  inherit = Object,
  public = list(
    read_dictionary = function(column_index) {
      parquet___arrow___ArrowReaderProperties__get_read_dictionary(self, column_index)
    },
    set_read_dictionary = function(column_index, read_dict) {
      parquet___arrow___ArrowReaderProperties__set_read_dictionary(self, column_index, read_dict)
    }
  ),
  active = list(
    use_threads = function(use_threads) {
      if(missing(use_threads)) {
        parquet___arrow___ArrowReaderProperties__get_use_threads(self)
      } else {
        parquet___arrow___ArrowReaderProperties__set_use_threads(self, use_threads)
      }
    }
  )
)

ParquetReaderProperties$create <- function(use_threads = option_use_threads()) {
  shared_ptr(
    ParquetReaderProperties,
    parquet___arrow___ArrowReaderProperties__Make(isTRUE(use_threads))
  )
}
