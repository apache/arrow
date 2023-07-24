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

#' Write a Feather file (an Arrow IPC file)
#'
#' Feather provides binary columnar serialization for data frames.
#' It is designed to make reading and writing data frames efficient,
#' and to make sharing data across data analysis languages easy.
#' [write_feather()] can write both the Feather Version 1 (V1),
#' a legacy version available starting in 2016, and the Version 2 (V2),
#' which is the Apache Arrow IPC file format.
#' The default version is V2.
#' V1 files are distinct from Arrow IPC files and lack many feathures,
#' such as the ability to store all Arrow data tyeps, and compression support.
#' [write_ipc_file()] can only write V2 files.
#'
#' @param x `data.frame`, [RecordBatch], or [Table]
#' @param sink A string file path, URI, or [OutputStream], or path in a file
#' system (`SubTreeFileSystem`)
#' @param version integer Feather file version, Version 1 or Version 2. Version 2 is the default.
#' @param chunk_size For V2 files, the number of rows that each chunk of data
#' should have in the file. Use a smaller `chunk_size` when you need faster
#' random row access. Default is 64K. This option is not supported for V1.
#' @param compression Name of compression codec to use, if any. Default is
#' "lz4" if LZ4 is available in your build of the Arrow C++ library, otherwise
#' "uncompressed". "zstd" is the other available codec and generally has better
#' compression ratios in exchange for slower read and write performance.
#' "lz4" is shorthand for the "lz4_frame" codec.
#' See [codec_is_available()] for details.
#' `TRUE` and `FALSE` can also be used in place of "default" and "uncompressed".
#' This option is not supported for V1.
#' @param compression_level If `compression` is "zstd", you may
#' specify an integer compression level. If omitted, the compression codec's
#' default compression level is used.
#'
#' @return The input `x`, invisibly. Note that if `sink` is an [OutputStream],
#' the stream will be left open.
#' @export
#' @seealso [RecordBatchWriter] for lower-level access to writing Arrow IPC data.
#' @seealso [Schema] for information about schemas and metadata handling.
#' @examples
#' # We recommend the ".arrow" extension for Arrow IPC files (Feather V2).
#' tf1 <- tempfile(fileext = ".feather")
#' tf2 <- tempfile(fileext = ".arrow")
#' tf3 <- tempfile(fileext = ".arrow")
#' on.exit({
#'   unlink(tf1)
#'   unlink(tf2)
#'   unlink(tf3)
#' })
#' write_feather(mtcars, tf1, version = 1)
#' write_feather(mtcars, tf2)
#' write_ipc_file(mtcars, tf3)
#' @include arrow-object.R
write_feather <- function(x,
                          sink,
                          version = 2,
                          chunk_size = 65536L,
                          compression = c("default", "lz4", "lz4_frame", "uncompressed", "zstd"),
                          compression_level = NULL) {
  # Handle and validate options before touching data
  version <- as.integer(version)
  assert_that(version %in% 1:2)

  if (isTRUE(compression)) compression <- "default"
  if (isFALSE(compression)) compression <- "uncompressed"

  # TODO(ARROW-17221): if (missing(compression)), we could detect_compression(sink) here
  compression <- match.arg(compression)
  chunk_size <- as.integer(chunk_size)
  assert_that(chunk_size > 0)
  if (compression == "default") {
    if (version == 2 && codec_is_available("lz4")) {
      compression <- "lz4"
    } else {
      compression <- "uncompressed"
    }
  }
  if (is.null(compression_level)) {
    # Use -1 as sentinal for "default"
    compression_level <- -1L
  }
  compression_level <- as.integer(compression_level)
  # Now make sure that options make sense together
  if (version == 1) {
    if (chunk_size != 65536L) {
      stop("Feather version 1 does not support the 'chunk_size' option", call. = FALSE)
    }
    if (compression != "uncompressed") {
      stop("Feather version 1 does not support the 'compression' option", call. = FALSE)
    }
    if (compression_level != -1L) {
      stop("Feather version 1 does not support the 'compression_level' option", call. = FALSE)
    }
  }
  if (compression != "zstd" && compression_level != -1L) {
    stop("Can only specify a 'compression_level' when 'compression' is 'zstd'", call. = FALSE)
  }
  # Finally, add 1 to version because 2 means V1 and 3 means V2 :shrug:
  version <- version + 1L

  # "lz4" is the convenience
  if (compression == "lz4") {
    compression <- "lz4_frame"
  }

  compression <- compression_from_name(compression)

  x_out <- x
  x <- as_writable_table(x)

  if (!inherits(sink, "OutputStream")) {
    sink <- make_output_stream(sink)
    on.exit(sink$close())
  }
  ipc___WriteFeather__Table(sink, x, version, chunk_size, compression, compression_level)
  invisible(x_out)
}

#' @rdname write_feather
#' @export
write_ipc_file <- function(x,
                           sink,
                           chunk_size = 65536L,
                           compression = c("default", "lz4", "lz4_frame", "uncompressed", "zstd"),
                           compression_level = NULL) {
  mc <- match.call()
  mc$version <- 2
  mc[[1]] <- get("write_feather", envir = asNamespace("arrow"))
  eval.parent(mc)
}

#' Read a Feather file (an Arrow IPC file)
#'
#' Feather provides binary columnar serialization for data frames.
#' It is designed to make reading and writing data frames efficient,
#' and to make sharing data across data analysis languages easy.
#' [read_feather()] can read both the Feather Version 1 (V1), a legacy version available starting in 2016,
#' and the Version 2 (V2), which is the Apache Arrow IPC file format.
#' [read_ipc_file()] is an alias of [read_feather()].
#'
#' @inheritParams read_ipc_stream
#' @inheritParams read_delim_arrow
#' @inheritParams make_readable_file
#'
#' @return A `data.frame` if `as_data_frame` is `TRUE` (the default), or an
#' Arrow [Table] otherwise
#'
#' @export
#' @seealso [FeatherReader] and [RecordBatchReader] for lower-level access to reading Arrow IPC data.
#' @examples
#' # We recommend the ".arrow" extension for Arrow IPC files (Feather V2).
#' tf <- tempfile(fileext = ".arrow")
#' on.exit(unlink(tf))
#' write_feather(mtcars, tf)
#' df <- read_feather(tf)
#' dim(df)
#' # Can select columns
#' df <- read_feather(tf, col_select = starts_with("d"))
read_feather <- function(file, col_select = NULL, as_data_frame = TRUE, mmap = TRUE) {
  if (!inherits(file, "RandomAccessFile")) {
    # Compression is handled inside the IPC file format, so we don't need
    # to detect from the file extension and wrap in a CompressedInputStream
    # TODO: Why is this the only read_format() functions that allows passing
    # mmap to make_readable_file?
    file <- make_readable_file(file, mmap)
    on.exit(file$close())
  }
  reader <- FeatherReader$create(file)

  col_select <- enquo(col_select)

  columns <- if (!quo_is_null(col_select)) {
    sim_df <- as.data.frame(reader$schema)
    indices <- eval_select(col_select, sim_df)
    names(reader)[indices]
  }

  out <- tryCatch(
    reader$Read(columns),
    error = read_compressed_error
  )

  if (isTRUE(as_data_frame)) {
    df <- out$to_data_frame()
    out <- apply_arrow_r_metadata(df, out$metadata$r)
  }
  out
}

#' @rdname read_feather
#' @export
read_ipc_file <- read_feather

#' @title FeatherReader class
#' @rdname FeatherReader
#' @name FeatherReader
#' @docType class
#' @usage NULL
#' @format NULL
#' @description This class enables you to interact with Feather files. Create
#' one to connect to a file or other InputStream, and call `Read()` on it to
#' make an `arrow::Table`. See its usage in [`read_feather()`].
#'
#' @section Factory:
#'
#' The `FeatherReader$create()` factory method instantiates the object and
#' takes the following argument:
#'
#' - `file` an Arrow file connection object inheriting from `RandomAccessFile`.
#'
#' @section Methods:
#'
#' - `$Read(columns)`: Returns a `Table` of the selected columns, a vector of
#'   integer indices
#' - `$column_names`: Active binding, returns the column names in the Feather file
#' - `$schema`: Active binding, returns the schema of the Feather file
#' - `$version`: Active binding, returns `1` or `2`, according to the Feather
#'   file version
#'
#' @export
#' @include arrow-object.R
FeatherReader <- R6Class("FeatherReader",
  inherit = ArrowObject,
  public = list(
    Read = function(columns) {
      ipc___feather___Reader__Read(self, columns)
    },
    print = function(...) {
      cat("FeatherReader:\n")
      print(self$schema)
      invisible(self)
    }
  ),
  active = list(
    # versions are officially 2 for V1 and 3 for V2 :shrug:
    version = function() ipc___feather___Reader__version(self) - 1L,
    column_names = function() names(self$schema),
    schema = function() ipc___feather___Reader__schema(self)
  )
)

#' @export
names.FeatherReader <- function(x) x$column_names

FeatherReader$create <- function(file) {
  assert_is(file, "RandomAccessFile")
  ipc___feather___Reader__Open(file)
}
