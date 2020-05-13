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
#' @param sources Either:
#'   * a string path to a directory containing data files
#'   * a list of `Dataset` objects as created by this function
#'   * a list of `DatasetFactory` objects as created by [dataset_factory()].
#' @param schema [Schema] for the dataset. If `NULL` (the default), the schema
#' will be inferred from the data sources.
#' @param partitioning When `sources` is a file path, one of
#'   * a `Schema`, in which case the file paths relative to `sources` will be
#'    parsed, and path segments will be matched with the schema fields. For
#'    example, `schema(year = int16(), month = int8())` would create partitions
#'    for file paths like "2019/01/file.parquet", "2019/02/file.parquet", etc.
#'   * a character vector that defines the field names corresponding to those
#'    path segments (that is, you're providing the names that would correspond
#'    to a `Schema` but the types will be autodetected)
#'   * a `HivePartitioning` or `HivePartitioningFactory`, as returned
#'    by [hive_partition()] which parses explicit or autodetected fields from
#'    Hive-style path segments
#'   * `NULL` for no partitioning
#'
#' The default is to autodetect Hive-style partitions.
#' @param unify_schemas logical: should all data fragments (files, `Dataset`s)
#' be scanned in order to create a unified schema from them? If `FALSE`, only
#' the first fragment will be inspected for its schema. Use this
#' fast path when you know and trust that all fragments have an identical schema.
#' The default is `FALSE` when creating a dataset from a file path (because
#' there may be many files and scanning may be slow) but `TRUE` when `sources`
#' is a list of `Dataset`s (because there should be few `Dataset`s in the list
#' and their `Schema`s are already in memory).
#' @param ... additional arguments passed to `dataset_factory()` when
#' `sources` is a file path, otherwise ignored. These may include "format" to
#' indicate the file format, or other format-specific options.
#' @return A [Dataset] R6 object. Use `dplyr` methods on it to query the data,
#' or call [`$NewScan()`][Scanner] to construct a query directly.
#' @export
#' @seealso `vignette("dataset", package = "arrow")`
#' @include arrow-package.R
open_dataset <- function(sources,
                         schema = NULL,
                         partitioning = hive_partition(),
                         unify_schemas = NULL,
                         ...) {
  if (is_list_of(sources, "Dataset")) {
    if (is.null(schema)) {
      if (is.null(unify_schemas) || isTRUE(unify_schemas)) {
        # Default is to unify schemas here
        schema <- unify_schemas(schemas = map(sources, ~.$schema))
      } else {
        # Take the first one.
        schema <- sources[[1]]$schema
      }
    }
    # Enforce that all datasets have the same schema
    sources <- lapply(sources, function(x) {
      x$schema <- schema
      x
    })
    return(shared_ptr(UnionDataset, dataset___UnionDataset__create(sources, schema)))
  }
  factory <- DatasetFactory$create(sources, partitioning = partitioning, ...)
  # Default is _not_ to inspect/unify schemas
  factory$Finish(schema, isTRUE(unify_schemas))
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
#' * `selector`: A [FileSelector]
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
Dataset <- R6Class("Dataset", inherit = ArrowObject,
  public = list(
    ..dispatch = function() {
      type <- self$type
      if (type == "union") {
        shared_ptr(UnionDataset, self$pointer())
      } else if (type == "filesystem") {
        shared_ptr(FileSystemDataset, self$pointer())
      } else {
        self
      }
    },
    # @description
    # Start a new scan of the data
    # @return A [ScannerBuilder]
    NewScan = function() unique_ptr(ScannerBuilder, dataset___Dataset__NewScan(self)),
    ToString = function() self$schema$ToString()
  ),
  active = list(
    schema = function(schema) {
      if (missing(schema)) {
        shared_ptr(Schema, dataset___Dataset__schema(self))
      } else {
        assert_is(schema, "Schema")
        invisible(shared_ptr(Dataset, dataset___Dataset__ReplaceSchema(self, schema)))
      }
    },
    metadata = function() self$schema$metadata,
    num_rows = function() {
      warning("Number of rows unknown; returning NA", call. = FALSE)
      NA_integer_
    },
    num_cols = function() length(self$schema),
    # @description
    # Return the Dataset's type.
    type = function() dataset___Dataset__type_name(self)
  )
)
Dataset$create <- open_dataset

#' @export
names.Dataset <- function(x) names(x$schema)

#' @export
dim.Dataset <- function(x) c(x$num_rows, x$num_cols)

#' @export
c.Dataset <- function(...) Dataset$create(list(...))

#' @name FileSystemDataset
#' @rdname Dataset
#' @export
FileSystemDataset <- R6Class("FileSystemDataset", inherit = Dataset,
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
      shared_ptr(FileFormat, dataset___FileSystemDataset__format(self))$..dispatch()
    },
    num_rows = function() {
      if (!inherits(self$format, "ParquetFileFormat")) {
        # TODO: implement for other file formats
        warning("Number of rows unknown; returning NA", call. = FALSE)
        NA_integer_
      } else {
        sum(map_int(self$files, ~ParquetFileReader$create(.x)$num_rows))
      }
    }
  )
)

#' @name UnionDataset
#' @rdname Dataset
#' @export
UnionDataset <- R6Class("UnionDataset", inherit = Dataset,
  active = list(
    # @description
    # Return the UnionDataset's child `Dataset`s
    children = function() {
      map(dataset___UnionDataset__children(self), ~shared_ptr(Dataset, .)$..dispatch())
    }
  )
)

#' @usage NULL
#' @format NULL
#' @rdname Dataset
#' @export
DatasetFactory <- R6Class("DatasetFactory", inherit = ArrowObject,
  public = list(
    Finish = function(schema = NULL, unify_schemas = FALSE) {
      if (is.null(schema)) {
        ptr <- dataset___DatasetFactory__Finish1(self, unify_schemas)
      } else {
        ptr <- dataset___DatasetFactory__Finish2(self, schema)
      }
      shared_ptr(Dataset, ptr)$..dispatch()
    },
    Inspect = function(unify_schemas = FALSE) {
      shared_ptr(Schema, dataset___DatasetFactory__Inspect(self, unify_schemas))
    }
  )
)
DatasetFactory$create <- function(x,
                                  filesystem = NULL,
                                  format = c("parquet", "arrow", "ipc", "feather", "csv", "tsv", "text"),
                                  partitioning = NULL,
                                  ...) {
  if (is_list_of(x, "DatasetFactory")) {
    return(shared_ptr(DatasetFactory, dataset___UnionDatasetFactory__Make(x)))
  }
  if (!is.string(x)) {
    stop("'x' must be a string or a list of DatasetFactory", call. = FALSE)
  }

  if (!inherits(filesystem, "FileSystem")) {
    if (grepl("://", x)) {
      fs_from_uri <- FileSystem$from_uri(x)
      filesystem <- fs_from_uri$fs
      x <- fs_from_uri$path
    } else {
      filesystem <- LocalFileSystem$create()
    }
  }
  selector <- FileSelector$create(x, allow_not_found = FALSE, recursive = TRUE)

  if (is.character(format)) {
    format <- FileFormat$create(match.arg(format), ...)
  } else {
    assert_is(format, "FileFormat")
  }

  if (!is.null(partitioning)) {
    if (inherits(partitioning, "Schema")) {
      partitioning <- DirectoryPartitioning$create(partitioning)
    } else if (is.character(partitioning)) {
      # These are the column/field names, and we should autodetect their types
      partitioning <- DirectoryPartitioningFactory$create(partitioning)
    }
  }
  FileSystemDatasetFactory$create(filesystem, selector, format, partitioning)
}


#' Create a DatasetFactory
#'
#' A [Dataset] can constructed using one or more [DatasetFactory]s.
#' This function helps you construct a `DatasetFactory` that you can pass to
#' [open_dataset()].
#'
#' If you would only have a single `DatasetFactory` (for example, you have a
#' single directory containing Parquet files), you can call `open_dataset()`
#' directly. Use `dataset_factory()` when you
#' want to combine different directories, file systems, or file formats.
#'
#' @param x A string file x containing data files, or
#' a list of `DatasetFactory` objects whose datasets should be
#' grouped. If this argument is specified it will be used to construct a
#' `UnionDatasetFactory` and other arguments will be ignored.
#' @param filesystem A [FileSystem] object; if omitted, the `FileSystem` will
#' be detected from `x`
#' @param format A [FileFormat] object, or a string identifier of the format of
#' the files in `x`. Currently supported values:
#' * "parquet"
#' * "ipc"/"arrow"/"feather", all aliases for each other; for Feather, note that
#'   only version 2 files are supported
#' * "csv"/"text", aliases for the same thing (because comma is the default
#'   delimiter for text files
#' * "tsv", equivalent to passing `format = "text", delimiter = "\t"`
#'
#' Default is "parquet", unless a `delimiter` is also specified, in which case
#' it is assumed to be "text".
#' @param partitioning One of
#'   * A `Schema`, in which case the file paths relative to `sources` will be
#'    parsed, and path segments will be matched with the schema fields. For
#'    example, `schema(year = int16(), month = int8())` would create partitions
#'    for file paths like "2019/01/file.parquet", "2019/02/file.parquet", etc.
#'   * A character vector that defines the field names corresponding to those
#'    path segments (that is, you're providing the names that would correspond
#'    to a `Schema` but the types will be autodetected)
#'   * A `HivePartitioning` or `HivePartitioningFactory`, as returned
#'    by [hive_partition()] which parses explicit or autodetected fields from
#'    Hive-style path segments
#'   * `NULL` for no partitioning
#' @param ... Additional format-specific options, passed to
#' `FileFormat$create()`. For CSV options, note that you can specify them either
#' with the Arrow C++ library naming ("delimiter", "quoting", etc.) or the
#' `readr`-style naming used in [read_csv_arrow()] ("delim", "quote", etc.)
#' @return A `DatasetFactory` object. Pass this to [open_dataset()],
#' in a list potentially with other `DatasetFactory` objects, to create
#' a `Dataset`.
#' @export
dataset_factory <- DatasetFactory$create

#' @usage NULL
#' @format NULL
#' @rdname Dataset
#' @export
FileSystemDatasetFactory <- R6Class("FileSystemDatasetFactory",
  inherit = DatasetFactory
)
FileSystemDatasetFactory$create <- function(filesystem,
                                            selector,
                                            format,
                                            partitioning = NULL) {
  assert_is(filesystem, "FileSystem")
  assert_is(selector, "FileSelector")
  assert_is(format, "FileFormat")

  if (is.null(partitioning)) {
    ptr <- dataset___FileSystemDatasetFactory__Make1(filesystem, selector, format)
  } else if (inherits(partitioning, "PartitioningFactory")) {
    ptr <- dataset___FileSystemDatasetFactory__Make3(filesystem, selector, format, partitioning)
  } else if (inherits(partitioning, "Partitioning")) {
    ptr <- dataset___FileSystemDatasetFactory__Make2(filesystem, selector, format, partitioning)
  } else {
    stop(
      "Expected 'partitioning' to be NULL, PartitioningFactory or Partitioning",
      call. = FALSE
    )
  }

  shared_ptr(FileSystemDatasetFactory, ptr)
}

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
                                     dict_columns = character(0)) {
  shared_ptr(ParquetFileFormat, dataset___ParquetFileFormat__Make(
    use_buffered_stream, buffer_size, dict_columns))
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

#' Scan the contents of a dataset
#'
#' @description
#' A `Scanner` iterates over a [Dataset]'s fragments and returns data
#' according to given row filtering and column projection. A `ScannerBuilder`
#' can help create one.
#'
#' @section Factory:
#' `Scanner$create()` wraps the `ScannerBuilder` interface to make a `Scanner`.
#' It takes the following arguments:
#'
#' * `dataset`: A `Dataset` or `arrow_dplyr_query` object, as returned by the
#'    `dplyr` methods on `Dataset`.
#' * `projection`: A character vector of column names to select
#' * `filter`: A `Expression` to filter the scanned rows by, or `TRUE` (default)
#'    to keep all rows.
#' * `use_threads`: logical: should scanning use multithreading? Default `TRUE`
#' * `...`: Additional arguments, currently ignored
#' @section Methods:
#' `ScannerBuilder` has the following methods:
#'
#' - `$Project(cols)`: Indicate that the scan should only return columns given
#' by `cols`, a character vector of column names
#' - `$Filter(expr)`: Filter rows by an [Expression].
#' - `$UseThreads(threads)`: logical: should the scan use multithreading?
#' The method's default input is `TRUE`, but you must call the method to enable
#' multithreading because the scanner default is `FALSE`.
#' - `$BatchSize(batch_size)`: integer: Maximum row count of scanned record
#' batches, default is 32K. If scanned record batches are overflowing memory
#' then this method can be called to reduce their size.
#' - `$schema`: Active binding, returns the [Schema] of the Dataset
#' - `$Finish()`: Returns a `Scanner`
#'
#' `Scanner` currently has a single method, `$ToTable()`, which evaluates the
#' query and returns an Arrow [Table].
#' @rdname Scanner
#' @name Scanner
#' @export
Scanner <- R6Class("Scanner", inherit = ArrowObject,
  public = list(
    ToTable = function() shared_ptr(Table, dataset___Scanner__ToTable(self)),
    Scan = function() map(dataset___Scanner__Scan(self), shared_ptr, class = ScanTask)
  )
)
Scanner$create <- function(dataset,
                           projection = NULL,
                           filter = TRUE,
                           use_threads = option_use_threads(),
                           ...) {
  if (inherits(dataset, "arrow_dplyr_query") && inherits(dataset$.data, "Dataset")) {
    return(Scanner$create(
      dataset$.data,
      dataset$selected_columns,
      dataset$filtered_rows,
      use_threads,
      ...
    ))
  }
  assert_is(dataset, "Dataset")
  scanner_builder <- dataset$NewScan()
  if (use_threads) {
    scanner_builder$UseThreads()
  }
  if (!is.null(projection)) {
    scanner_builder$Project(projection)
  }
  if (!isTRUE(filter)) {
    scanner_builder$Filter(filter)
  }
  scanner_builder$Finish()
}

ScanTask <- R6Class("ScanTask", inherit = ArrowObject,
  public = list(
    Execute = function() map(dataset___ScanTask__get_batches(self), shared_ptr, class = RecordBatch)
  )
)

#' Apply a function to a stream of RecordBatches
#'
#' As an alternative to calling `collect()` on a `Dataset` query, you can
#' use this function to access the stream of `RecordBatch`es in the `Dataset`.
#' This lets you aggregate on each chunk and pull the intermediate results into
#' a `data.frame` for further aggregation, even if you couldn't fit the whole
#' `Dataset` result in memory.
#'
#' This is experimental and not recommended for production use.
#'
#' @param X A `Dataset` or `arrow_dplyr_query` object, as returned by the
#' `dplyr` methods on `Dataset`.
#' @param FUN A function or `purrr`-style lambda expression to apply to each
#' batch
#' @param ... Additional arguments passed to `FUN`
#' @param .data.frame logical: collect the resulting chunks into a single
#' `data.frame`? Default `TRUE`
#' @export
map_batches <- function(X, FUN, ..., .data.frame = TRUE) {
  if (.data.frame) {
    lapply <- map_dfr
  }
  scanner <- Scanner$create(ensure_group_vars(X))
  FUN <- as_mapper(FUN)
  # message("Making ScanTasks")
  lapply(scanner$Scan(), function(scan_task) {
    # This outer lapply could be parallelized
    # message("Making Batches")
    lapply(scan_task$Execute(), function(batch) {
      # message("Processing Batch")
      # This inner lapply cannot be parallelized
      # TODO: wrap batch in arrow_dplyr_query with X$selected_columns and X$group_by_vars
      # if X is arrow_dplyr_query, if some other arg (.dplyr?) == TRUE
      FUN(batch, ...)
    })
  })
}

#' @usage NULL
#' @format NULL
#' @rdname Scanner
#' @export
ScannerBuilder <- R6Class("ScannerBuilder", inherit = ArrowObject,
  public = list(
    Project = function(cols) {
      assert_is(cols, "character")
      dataset___ScannerBuilder__Project(self, cols)
      self
    },
    Filter = function(expr) {
      assert_is(expr, "Expression")
      dataset___ScannerBuilder__Filter(self, expr)
      self
    },
    UseThreads = function(threads = option_use_threads()) {
      dataset___ScannerBuilder__UseThreads(self, threads)
      self
    },
    BatchSize = function(batch_size) {
      dataset___ScannerBuilder__BatchSize(self, batch_size)
      self
    },
    Finish = function() unique_ptr(Scanner, dataset___ScannerBuilder__Finish(self))
  ),
  active = list(
    schema = function() shared_ptr(Schema, dataset___ScannerBuilder__schema(self))
  )
)

#' @export
names.ScannerBuilder <- function(x) names(x$schema)

#' Define Partitioning for a Dataset
#'
#' @description
#' Pass a `Partitioning` object to a [FileSystemDatasetFactory]'s `$create()`
#' method to indicate how the file's paths should be interpreted to define
#' partitioning.
#'
#' `DirectoryPartitioning` describes how to interpret raw path segments, in
#' order. For example, `schema(year = int16(), month = int8())` would define
#' partitions for file paths like "2019/01/file.parquet",
#' "2019/02/file.parquet", etc.
#'
#' `HivePartitioning` is for Hive-style partitioning, which embeds field
#' names and values in path segments, such as
#' "/year=2019/month=2/data.parquet". Because fields are named in the path
#' segments, order does not matter.
#'
#' `PartitioningFactory` subclasses instruct the `DatasetFactory` to detect
#' partition features from the file paths.
#' @section Factory:
#' Both `DirectoryPartitioning$create()` and `HivePartitioning$create()`
#' methods take a [Schema] as a single input argument. The helper
#' function [`hive_partition(...)`][hive_partition] is shorthand for
#' `HivePartitioning$create(schema(...))`.
#'
#' With `DirectoryPartitioningFactory$create()`, you can provide just the
#' names of the path segments (in our example, `c("year", "month")`), and
#' the `DatasetFactory` will infer the data types for those partition variables.
#' `HivePartitioningFactory$create()` takes no arguments: both variable names
#' and their types can be inferred from the file paths. `hive_partition()` with
#' no arguments returns a `HivePartitioningFactory`.
#' @name Partitioning
#' @rdname Partitioning
#' @export
Partitioning <- R6Class("Partitioning", inherit = ArrowObject)
#' @usage NULL
#' @format NULL
#' @rdname Partitioning
#' @export
DirectoryPartitioning <- R6Class("DirectoryPartitioning", inherit = Partitioning)
DirectoryPartitioning$create <- function(schema) {
  shared_ptr(DirectoryPartitioning, dataset___DirectoryPartitioning(schema))
}

#' @usage NULL
#' @format NULL
#' @rdname Partitioning
#' @export
HivePartitioning <- R6Class("HivePartitioning", inherit = Partitioning)
HivePartitioning$create <- function(schema) {
  shared_ptr(HivePartitioning, dataset___HivePartitioning(schema))
}

#' Construct Hive partitioning
#'
#' Hive partitioning embeds field names and values in path segments, such as
#' "/year=2019/month=2/data.parquet".
#'
#' Because fields are named in the path segments, order of fields passed to
#' `hive_partition()` does not matter.
#' @param ... named list of [data types][data-type], passed to [schema()]
#' @return A [HivePartitioning][Partitioning], or a `HivePartitioningFactory` if
#' calling `hive_partition()` with no arguments.
#' @examples
#' \donttest{
#' hive_partition(year = int16(), month = int8())
#' }
#' @export
hive_partition <- function(...) {
  schm <- schema(...)
  if (length(schm) == 0) {
    HivePartitioningFactory$create()
  } else {
    HivePartitioning$create(schm)
  }
}

PartitioningFactory <- R6Class("PartitioningFactory", inherit = ArrowObject)

#' @usage NULL
#' @format NULL
#' @rdname Partitioning
#' @export
DirectoryPartitioningFactory <- R6Class("DirectoryPartitioningFactory ", inherit = PartitioningFactory)
DirectoryPartitioningFactory$create <- function(x) {
  shared_ptr(DirectoryPartitioningFactory, dataset___DirectoryPartitioning__MakeFactory(x))
}

#' @usage NULL
#' @format NULL
#' @rdname Partitioning
#' @export
HivePartitioningFactory <- R6Class("HivePartitioningFactory", inherit = PartitioningFactory)
HivePartitioningFactory$create <- function() {
  shared_ptr(HivePartitioningFactory, dataset___HivePartitioning__MakeFactory())
}
