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

#' Open a multi-file dataset
#'
#' Arrow Datasets allow you to query against data that has been split across
#' multiple files. This sharding of data may indicate partitioning, which
#' can accelerate queries that only touch some partitions (files). Call
#' `open_dataset()` to point to a directory of data files and return a
#' `Dataset`, then use `dplyr` methods to query it.
#'
#' @section Partitioning:
#'
#' Data is often split into multiple files and nested in subdirectories based on the value of one or more
#' columns in the data. It may be a column that is commonly referenced in
#' queries, or it may be time-based, for some examples. Data that is divided
#' this way is "partitioned," and the values for those partitioning columns are
#' encoded into the file path segments.
#' These path segments are effectively virtual columns in the dataset, and
#' because their values are known prior to reading the files themselves, we can
#' greatly speed up filtered queries by skipping some files entirely.
#'
#' Arrow supports reading partition information from file paths in two forms:
#'
#' * "Hive-style", deriving from the Apache Hive project and common to some
#'   database systems. Partitions are encoded as "key=value" in path segments,
#'   such as `"year=2019/month=1/file.parquet"`. While they may be awkward as
#'   file names, they have the advantage of being self-describing.
#' * "Directory" partitioning, which is Hive without the key names, like
#'   `"2019/01/file.parquet"`. In order to use these, we need know at least
#'   what names to give the virtual columns that come from the path segments.
#'
#' The default behavior in `open_dataset()` is to inspect the file paths
#' contained in the provided directory, and if they look like Hive-style, parse
#' them as Hive. If your dataset has Hive-style partioning in the file paths,
#' you do not need to provide anything in the `partitioning` argument to
#' `open_dataset()` to use them. If you do provide a character vector of
#' partition column names, they will be ignored if they match what is detected,
#' and if they don't match, you'll get an error. (If you want to rename
#' partition columns, do that using `select()` or `rename()` after opening the
#' dataset.). If you provide a `Schema` and the names match what is detected,
#' it will use the types defined by the Schema. In the example file path above,
#' you could provide a Schema to specify that "month" should be `int8()`
#' instead of the `int32()` it will be parsed as by default.
#'
#' If your file paths do not appear to be Hive-style, or if you pass
#' `hive_style = FALSE`, the `partitioning` argument will be used to create
#' Directory partitioning. A character vector of names is required to create
#' partitions; you may instead provide a `Schema` to map those names to desired
#' column types, as described above. If neither are provided, no partitioning
#' information will be taken from the file paths.
#'
#' @param sources One of:
#'   * a string path or URI to a directory containing data files
#'   * a [FileSystem] that references a directory containing data files
#'     (such as what is returned by [s3_bucket()])
#'   * a string path or URI to a single file
#'   * a character vector of paths or URIs to individual data files
#'   * a list of `Dataset` objects as created by this function
#'   * a list of `DatasetFactory` objects as created by [dataset_factory()].
#'
#' When `sources` is a vector of file URIs, they must all use the same protocol
#' and point to files located in the same file system and having the same
#' format.
#' @param schema [Schema] for the `Dataset`. If `NULL` (the default), the schema
#' will be inferred from the data sources.
#' @param partitioning When `sources` is a directory path/URI, one of:
#'   * a `Schema`, in which case the file paths relative to `sources` will be
#'     parsed, and path segments will be matched with the schema fields.
#'   * a character vector that defines the field names corresponding to those
#'     path segments (that is, you're providing the names that would correspond
#'     to a `Schema` but the types will be autodetected)
#'   * a `Partitioning` or `PartitioningFactory`, such as returned
#'     by [hive_partition()]
#'   * `NULL` for no partitioning
#'
#' The default is to autodetect Hive-style partitions unless
#' `hive_style = FALSE`. See the "Partitioning" section for details.
#' When `sources` is not a directory path/URI, `partitioning` is ignored.
#' @param hive_style Logical: should `partitioning` be interpreted as
#' Hive-style? Default is `NA`, which means to inspect the file paths for
#' Hive-style partitioning and behave accordingly.
#' @param unify_schemas logical: should all data fragments (files, `Dataset`s)
#' be scanned in order to create a unified schema from them? If `FALSE`, only
#' the first fragment will be inspected for its schema. Use this fast path
#' when you know and trust that all fragments have an identical schema.
#' The default is `FALSE` when creating a dataset from a directory path/URI or
#' vector of file paths/URIs (because there may be many files and scanning may
#' be slow) but `TRUE` when `sources` is a list of `Dataset`s (because there
#' should be few `Dataset`s in the list and their `Schema`s are already in
#' memory).
#' @param format A [FileFormat] object, or a string identifier of the format of
#' the files in `x`. This argument is ignored when `sources` is a list of `Dataset` objects.
#' Currently supported values:
#' * "parquet"
#' * "ipc"/"arrow"/"feather", all aliases for each other; for Feather, note that
#'   only version 2 files are supported
#' * "csv"/"text", aliases for the same thing (because comma is the default
#'   delimiter for text files
#' * "tsv", equivalent to passing `format = "text", delimiter = "\t"`
#'
#' Default is "parquet", unless a `delimiter` is also specified, in which case
#' it is assumed to be "text".
#' @param ... additional arguments passed to `dataset_factory()` when `sources`
#' is a directory path/URI or vector of file paths/URIs, otherwise ignored.
#' These may include `format` to indicate the file format, or other
#' format-specific options (see [read_csv_arrow()], [read_parquet()] and [read_feather()] on how to specify these).
#' @inheritParams dataset_factory
#' @return A [Dataset] R6 object. Use `dplyr` methods on it to query the data,
#' or call [`$NewScan()`][Scanner] to construct a query directly.
#' @export
#' @seealso \href{https://arrow.apache.org/docs/r/articles/dataset.html}{
#' datasets article}
#' @include arrow-object.R
#' @examplesIf arrow_with_dataset() & arrow_with_parquet()
#' # Set up directory for examples
#' tf <- tempfile()
#' dir.create(tf)
#' on.exit(unlink(tf))
#'
#' write_dataset(mtcars, tf, partitioning = "cyl")
#'
#' # You can specify a directory containing the files for your dataset and
#' # open_dataset will scan all files in your directory.
#' open_dataset(tf)
#'
#' # You can also supply a vector of paths
#' open_dataset(c(file.path(tf, "cyl=4/part-0.parquet"), file.path(tf, "cyl=8/part-0.parquet")))
#'
#' ## You must specify the file format if using a format other than parquet.
#' tf2 <- tempfile()
#' dir.create(tf2)
#' on.exit(unlink(tf2))
#' write_dataset(mtcars, tf2, format = "ipc")
#' # This line will results in errors when you try to work with the data
#' \dontrun{
#' open_dataset(tf2)
#' }
#' # This line will work
#' open_dataset(tf2, format = "ipc")
#'
#' ## You can specify file partitioning to include it as a field in your dataset
#' # Create a temporary directory and write example dataset
#' tf3 <- tempfile()
#' dir.create(tf3)
#' on.exit(unlink(tf3))
#' write_dataset(airquality, tf3, partitioning = c("Month", "Day"), hive_style = FALSE)
#'
#' # View files - you can see the partitioning means that files have been written
#' # to folders based on Month/Day values
#' tf3_files <- list.files(tf3, recursive = TRUE)
#'
#' # With no partitioning specified, dataset contains all files but doesn't include
#' # directory names as field names
#' open_dataset(tf3)
#'
#' # Now that partitioning has been specified, your dataset contains columns for Month and Day
#' open_dataset(tf3, partitioning = c("Month", "Day"))
#'
#' # If you want to specify the data types for your fields, you can pass in a Schema
#' open_dataset(tf3, partitioning = schema(Month = int8(), Day = int8()))
open_dataset <- function(sources,
                         schema = NULL,
                         partitioning = hive_partition(),
                         hive_style = NA,
                         unify_schemas = NULL,
                         format = c("parquet", "arrow", "ipc", "feather", "csv", "tsv", "text"),
                         factory_options = list(),
                         ...) {
  stop_if_no_datasets()

  if (is_list_of(sources, "Dataset")) {
    if (is.null(schema)) {
      if (is.null(unify_schemas) || isTRUE(unify_schemas)) {
        # Default is to unify schemas here
        schema <- unify_schemas(schemas = map(sources, ~ .$schema))
      } else {
        # Take the first one.
        schema <- sources[[1]]$schema
      }
    }
    # Enforce that all datasets have the same schema
    assert_is(schema, "Schema")
    sources <- lapply(sources, function(x) {
      x$WithSchema(schema)
    })
    return(dataset___UnionDataset__create(sources, schema))
  }

  if (is_false(hive_style) &&
    inherits(partitioning, "PartitioningFactory") &&
    identical(partitioning$type_name, "hive")) {
    # Allow default partitioning arg to be overridden by hive_style = FALSE
    partitioning <- NULL
  }

  factory <- DatasetFactory$create(
    sources,
    partitioning = partitioning,
    format = format,
    schema = schema,
    hive_style = hive_style,
    factory_options = factory_options,
    ...
  )
  tryCatch(
    # Default is _not_ to inspect/unify schemas
    factory$Finish(schema, isTRUE(unify_schemas)),
    # n = 4 because we want the error to show up as being from open_dataset()
    # and not augment_io_error_msg()
    error = function(e, call = caller_env(n = 4)) {
      augment_io_error_msg(e, call, format = format)
    }
  )
}

#' Open a multi-file dataset of CSV or other delimiter-separated format
#'
#' A wrapper around [open_dataset] which explicitly includes parameters mirroring [read_csv_arrow()],
#' [read_delim_arrow()], and [read_tsv_arrow()] to allow for easy switching between functions
#' for opening single files and functions for opening datasets.
#'
#' @inheritParams open_dataset
#' @inheritParams read_delim_arrow
#'
#' @section Options currently supported by [read_delim_arrow()] which are not supported here:
#' * `file` (instead, please specify files in `sources`)
#' * `col_select` (instead, subset columns after dataset creation)
#' * `quoted_na`
#' * `as_data_frame` (instead, convert to data frame after dataset creation)
#' * `parse_options`
#'
#' @examplesIf arrow_with_dataset()
#' # Set up directory for examples
#' tf <- tempfile()
#' dir.create(tf)
#' df <- data.frame(x = c("1", "2", "NULL"))
#'
#' file_path <- file.path(tf, "file1.txt")
#' write.table(df, file_path, sep = ",", row.names = FALSE)
#'
#' read_csv_arrow(file_path, na = c("", "NA", "NULL"), col_names = "y", skip = 1)
#' open_csv_dataset(file_path, na = c("", "NA", "NULL"), col_names = "y", skip = 1)
#'
#' unlink(tf)
#' @seealso [open_dataset()]
#' @export
open_delim_dataset <- function(sources,
                               schema = NULL,
                               partitioning = hive_partition(),
                               hive_style = NA,
                               unify_schemas = NULL,
                               factory_options = list(),
                               delim = ",",
                               quote = "\"",
                               escape_double = TRUE,
                               escape_backslash = FALSE,
                               col_names = TRUE,
                               col_types = NULL,
                               na = c("", "NA"),
                               skip_empty_rows = TRUE,
                               skip = 0L,
                               convert_options = NULL,
                               read_options = NULL,
                               timestamp_parsers = NULL) {
  open_dataset(
    sources = sources,
    schema = schema,
    partitioning = partitioning,
    hive_style = hive_style,
    unify_schemas = unify_schemas,
    factory_options = factory_options,
    format = "text",
    delim = delim,
    quote = quote,
    escape_double = escape_double,
    escape_backslash = escape_backslash,
    col_names = col_names,
    col_types = col_types,
    na = na,
    skip_empty_rows = skip_empty_rows,
    skip = skip,
    convert_options = convert_options,
    read_options = read_options,
    timestamp_parsers = timestamp_parsers
  )
}

#' @rdname open_delim_dataset
#' @export
open_csv_dataset <- function(sources,
                             schema = NULL,
                             partitioning = hive_partition(),
                             hive_style = NA,
                             unify_schemas = NULL,
                             factory_options = list(),
                             quote = "\"",
                             escape_double = TRUE,
                             escape_backslash = FALSE,
                             col_names = TRUE,
                             col_types = NULL,
                             na = c("", "NA"),
                             skip_empty_rows = TRUE,
                             skip = 0L,
                             convert_options = NULL,
                             read_options = NULL,
                             timestamp_parsers = NULL) {
  mc <- match.call()
  mc$delim <- ","
  mc[[1]] <- get("open_delim_dataset", envir = asNamespace("arrow"))
  eval.parent(mc)
}

#' @rdname open_delim_dataset
#' @export
open_tsv_dataset <- function(sources,
                             schema = NULL,
                             partitioning = hive_partition(),
                             hive_style = NA,
                             unify_schemas = NULL,
                             factory_options = list(),
                             quote = "\"",
                             escape_double = TRUE,
                             escape_backslash = FALSE,
                             col_names = TRUE,
                             col_types = NULL,
                             na = c("", "NA"),
                             skip_empty_rows = TRUE,
                             skip = 0L,
                             convert_options = NULL,
                             read_options = NULL,
                             timestamp_parsers = NULL) {
  mc <- match.call()
  mc$delim <- "\t"
  mc[[1]] <- get("open_delim_dataset", envir = asNamespace("arrow"))
  eval.parent(mc)
}



#' Multi-file datasets
#'
#' @description
#' Arrow Datasets allow you to query against data that has been split across
#' multiple files. This sharding of data may indicate partitioning, which
#' can accelerate queries that only touch some partitions (files).
#'
#' A `Dataset` contains one or more `Fragments`, such as files, of potentially
#' differing type and partitioning.
#'
#' For `Dataset$create()`, see [open_dataset()], which is an alias for it.
#'
#' `DatasetFactory` is used to provide finer control over the creation of `Dataset`s.
#'
#' @section Factory:
#' `DatasetFactory` is used to create a `Dataset`, inspect the [Schema] of the
#' fragments contained in it, and declare a partitioning.
#' `FileSystemDatasetFactory` is a subclass of `DatasetFactory` for
#' discovering files in the local file system, the only currently supported
#' file system.
#'
#' For the `DatasetFactory$create()` factory method, see [dataset_factory()], an
#' alias for it. A `DatasetFactory` has:
#'
#' - `$Inspect(unify_schemas)`: If `unify_schemas` is `TRUE`, all fragments
#' will be scanned and a unified [Schema] will be created from them; if `FALSE`
#' (default), only the first fragment will be inspected for its schema. Use this
#' fast path when you know and trust that all fragments have an identical schema.
#' - `$Finish(schema, unify_schemas)`: Returns a `Dataset`. If `schema` is provided,
#' it will be used for the `Dataset`; if omitted, a `Schema` will be created from
#' inspecting the fragments (files) in the dataset, following `unify_schemas`
#' as described above.
#'
#' `FileSystemDatasetFactory$create()` is a lower-level factory method and
#' takes the following arguments:
#' * `filesystem`: A [FileSystem]
#' * `selector`: Either a [FileSelector] or `NULL`
#' * `paths`: Either a character vector of file paths or `NULL`
#' * `format`: A [FileFormat]
#' * `partitioning`: Either `Partitioning`, `PartitioningFactory`, or `NULL`
#' @section Methods:
#'
#' A `Dataset` has the following methods:
#' - `$NewScan()`: Returns a [ScannerBuilder] for building a query
#' - `$WithSchema()`: Returns a new Dataset with the specified schema.
#'   This method currently supports only adding, removing, or reordering
#'   fields in the schema: you cannot alter or cast the field types.
#' - `$schema`: Active binding that returns the [Schema] of the Dataset; you
#'   may also replace the dataset's schema by using `ds$schema <- new_schema`.
#'
#' `FileSystemDataset` has the following methods:
#' - `$files`: Active binding, returns the files of the `FileSystemDataset`
#' - `$format`: Active binding, returns the [FileFormat] of the `FileSystemDataset`
#'
#' `UnionDataset` has the following methods:
#' - `$children`: Active binding, returns all child `Dataset`s.
#'
#' @export
#' @seealso [open_dataset()] for a simple interface to creating a `Dataset`
Dataset <- R6Class("Dataset",
  inherit = ArrowObject,
  public = list(
    # @description
    # Start a new scan of the data
    # @return A [ScannerBuilder]
    NewScan = function() dataset___Dataset__NewScan(self),
    ToString = function() self$schema$ToString(),
    WithSchema = function(schema) {
      assert_is(schema, "Schema")
      dataset___Dataset__ReplaceSchema(self, schema)
    }
  ),
  active = list(
    schema = function(schema) {
      if (missing(schema)) {
        dataset___Dataset__schema(self)
      } else {
        out <- self$WithSchema(schema)
        # WithSchema returns a new object but we're modifying in place,
        # so swap in that new C++ object pointer into our R6 object
        self$set_pointer(out$pointer())
        self
      }
    },
    metadata = function() self$schema$metadata,
    num_rows = function() self$NewScan()$Finish()$CountRows(),
    num_cols = function() length(self$schema),
    # @description
    # Return the Dataset's type.
    type = function() dataset___Dataset__type_name(self)
  )
)
Dataset$create <- open_dataset

#' @name FileSystemDataset
#' @rdname Dataset
#' @export
FileSystemDataset <- R6Class("FileSystemDataset",
  inherit = Dataset,
  public = list(
    .class_title = function() {
      nfiles <- length(self$files)
      file_type <- self$format$type
      pretty_file_type <- list(
        parquet = "Parquet",
        ipc = "Feather"
      )[[file_type]]

      paste(
        class(self)[[1]],
        "with",
        nfiles,
        pretty_file_type %||% file_type,
        ifelse(nfiles == 1, "file", "files")
      )
    }
  ),
  active = list(
    # @description
    # Return the files contained in this `FileSystemDataset`
    files = function() dataset___FileSystemDataset__files(self),
    # @description
    # Return the format of files in this `Dataset`
    format = function() {
      dataset___FileSystemDataset__format(self)
    },
    # @description
    # Return the filesystem of files in this `Dataset`
    filesystem = function() {
      dataset___FileSystemDataset__filesystem(self)
    }
  )
)

#' @name UnionDataset
#' @rdname Dataset
#' @export
UnionDataset <- R6Class("UnionDataset",
  inherit = Dataset,
  active = list(
    # @description
    # Return the UnionDataset's child `Dataset`s
    children = function() {
      dataset___UnionDataset__children(self)
    }
  )
)

#' @name InMemoryDataset
#' @rdname Dataset
#' @export
InMemoryDataset <- R6Class("InMemoryDataset", inherit = Dataset)
InMemoryDataset$create <- function(x) {
  stop_if_no_datasets()
  if (!inherits(x, "Table")) {
    x <- Table$create(x)
  }
  dataset___InMemoryDataset__create(x)
}


#' @export
names.Dataset <- function(x) names(x$schema)

#' @export
dim.Dataset <- function(x) c(x$num_rows, x$num_cols)

#' @export
c.Dataset <- function(...) Dataset$create(list(...))

#' @export
as.data.frame.Dataset <- function(x, row.names = NULL, optional = FALSE, ...) {
  collect.Dataset(x)
}

#' @export
head.Dataset <- function(x, n = 6L, ...) {
  head(Scanner$create(x), n)
}

#' @export
tail.Dataset <- function(x, n = 6L, ...) {
  tail(Scanner$create(x), n)
}

#' @export
`[.Dataset` <- function(x, i, j, ..., drop = FALSE) {
  if (nargs() == 2L) {
    # List-like column extraction (x[i])
    return(x[, i])
  }
  if (!missing(j)) {
    x <- select.Dataset(x, all_of(j))
  }

  if (!missing(i)) {
    x <- take_dataset_rows(x, i)
  }
  x
}

take_dataset_rows <- function(x, i) {
  if (!is.numeric(i) || any(i < 0)) {
    stop("Only slicing with positive indices is supported", call. = FALSE)
  }
  scanner <- Scanner$create(x)
  i <- Array$create(i - 1)
  dataset___Scanner__TakeRows(scanner, i)
}

stop_if_no_datasets <- function() {
  if (!arrow_with_dataset()) {
    stop("This build of the arrow package does not support Datasets", call. = FALSE)
  }
}
