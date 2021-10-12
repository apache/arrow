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
#' @param sources One of:
#'   * a string path or URI to a directory containing data files
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
#'    parsed, and path segments will be matched with the schema fields. For
#'    example, `schema(year = int16(), month = int8())` would create partitions
#'    for file paths like `"2019/01/file.parquet"`, `"2019/02/file.parquet"`,
#'    etc.
#'   * a character vector that defines the field names corresponding to those
#'    path segments (that is, you're providing the names that would correspond
#'    to a `Schema` but the types will be autodetected)
#'   * a `HivePartitioning` or `HivePartitioningFactory`, as returned
#'    by [hive_partition()] which parses explicit or autodetected fields from
#'    Hive-style path segments
#'   * `NULL` for no partitioning
#'
#' The default is to autodetect Hive-style partitions. When `sources` is not a
#' directory path/URI, `partitioning` is ignored.
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
#' format-specific options.
#' @return A [Dataset] R6 object. Use `dplyr` methods on it to query the data,
#' or call [`$NewScan()`][Scanner] to construct a query directly.
#' @export
#' @seealso `vignette("dataset", package = "arrow")`
#' @include arrow-package.R
#' @examplesIf arrow_with_dataset() & arrow_with_parquet()
#' # Set up directory for examples
#' tf <- tempfile()
#' dir.create(tf)
#' on.exit(unlink(tf))
#'
#' data <- dplyr::group_by(mtcars, cyl)
#' write_dataset(data, tf)
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
#' write_dataset(data, tf2, format = "ipc")
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
                         unify_schemas = NULL,
                         format = c("parquet", "arrow", "ipc", "feather", "csv", "tsv", "text"),
                         ...) {
  if (!arrow_with_dataset()) {
    stop("This build of the arrow package does not support Datasets", call. = FALSE)
  }
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
      x$schema <- schema
      x
    })
    return(dataset___UnionDataset__create(sources, schema))
  }

  factory <- DatasetFactory$create(sources, partitioning = partitioning, format = format, ...)
  tryCatch(
    # Default is _not_ to inspect/unify schemas
    factory$Finish(schema, isTRUE(unify_schemas)),
    error = function(e) {
      handle_parquet_io_error(e, format)
    }
  )
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
#' - `$schema`: Active binding that returns the [Schema] of the Dataset; you
#'   may also replace the dataset's schema by using `ds$schema <- new_schema`.
#'   This method currently supports only adding, removing, or reordering
#'   fields in the schema: you cannot alter or cast the field types.
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
    ToString = function() self$schema$ToString()
  ),
  active = list(
    schema = function(schema) {
      if (missing(schema)) {
        dataset___Dataset__schema(self)
      } else {
        assert_is(schema, "Schema")
        invisible(dataset___Dataset__ReplaceSchema(self, schema))
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
  if (!arrow_with_dataset()) {
    stop("This build of the arrow package does not support Datasets", call. = FALSE)
  }
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
