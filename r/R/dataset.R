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
#' @param sources Either (1) a string path to a directory containing data files,
#' or (2) a list of `DataSourceDiscovery` objects as created by [data_source()].
#' @param schema [Schema] for the dataset. If `NULL` (the default), the schema
#' will be inferred from the data sources.
#' @param partition When `sources` is a file path, one of
#'   * A `Schema`, in which case the file paths relative to `sources` will be
#'    parsed, and path segments will be matched with the schema fields. For
#'    example, `schema(year = int16(), month = int8())` would create partitions
#'    for file paths like "2019/01/file.parquet", "2019/02/file.parquet", etc.
#'   * A character vector that defines the field names corresponding to those
#'    path segments (that is, you're providing the names that would correspond
#'    to a `Schema` but the types will be autodetected)
#'   * A `HivePartitionScheme` or `HivePartitionSchemeDiscovery`, as returned
#'    by [hive_partition()] which parses explicit or autodetected fields from
#'    Hive-style path segments
#'   * `NULL` for no partitioning
#' @param ... additional arguments passed to `data_source()`, when `sources` is
#' a file path, otherwise ignored.
#' @return A [Dataset] R6 object. Use `dplyr` methods on it to query the data,
#' or call `$NewScan()` to construct a query directly.
#' @export
#' @seealso [PartitionScheme] for defining partitioning
#' @include arrow-package.R
open_dataset <- function(sources, schema = NULL, partition = hive_partition(), ...) {
  if (is.character(sources)) {
    sources <- list(data_source(sources, partition = partition, ...))
  }
  assert_is_list_of(sources, "DataSourceDiscovery")
  if (is.null(schema)) {
    # TODO: there should be a DatasetFactory
    # https://jira.apache.org/jira/browse/ARROW-7380
    schema <- sources[[1]]$Inspect()
  }
  Dataset$create(map(sources, ~.$Finish(schema)), schema)
}

#' Multi-file datasets
#'
#' @description
#' Arrow Datasets allow you to query against data that has been split across
#' multiple files. This sharding of data may indicate partitioning, which
#' can accelerate queries that only touch some partitions (files).
#'
#' @section Factory:
#' The `Dataset$create()` factory method instantiates a `Dataset` and
#' takes the following arguments:
#' * `sources`: a list of [DataSource] objects
#' * `schema`: a [Schema]
#' @section Methods:
#'
#' - `$NewScan()`: Returns a [ScannerBuilder] for building a query
#' - `$schema`: Active binding, returns the [Schema] of the Dataset
#' @export
#' @seealso [open_dataset()] for a simple way to create a Dataset that has a
#' single `DataSource`.
Dataset <- R6Class("Dataset", inherit = Object,
  public = list(
    #' @description
    #' Start a new scan of the data
    #' @return A [ScannerBuilder]
    NewScan = function() unique_ptr(ScannerBuilder, dataset___Dataset__NewScan(self))
  ),
  active = list(
    #' @description
    #' Return the Dataset's `Schema`
    schema = function() shared_ptr(Schema, dataset___Dataset__schema(self))
  )
)
Dataset$create <- function(sources, schema) {
  assert_is_list_of(sources, "DataSource")
  assert_is(schema, "Schema")
  shared_ptr(Dataset, dataset___Dataset__create(sources, schema))
}

#' @export
names.Dataset <- function(x) names(x$schema)

#' Data sources for a Dataset
#'
#' @description
#' A [Dataset] can have one or more `DataSource`s. A `DataSource` contains one
#' or more `DataFragments`, such as files, of a common type and partition
#' scheme. `DataSourceDiscovery` is used to create a `DataSource`, inspect the
#' [Schema] of the fragments contained in it, and declare a partition scheme.
#' `FileSystemDataSourceDiscovery` is a subclass of `DataSourceDiscovery` for
#' discovering files in the local file system, the only currently supported
#' file system.
#'
#' In general, you'll deal with `DataSourceDiscovery` rather than `DataSource`
#' itself.
#' @section Factory:
#' For the `DataSourceDiscovery$create()` factory method, see [data_source()],
#' an alias for it.
#'
#' `FileSystemDataSourceDiscovery$create()` is a lower-level factory method and
#' takes the following arguments:
#' * `filesystem`: A [FileSystem]
#' * `selector`: A [FileSelector]
#' * `format`: Currently only "parquet" is supported
#' @section Methods:
#' `DataSource` has one defined method:
#'
#' - `$schema`: Active binding, returns the [Schema] of the DataSource
#'
#' `DataSourceDiscovery` and its subclasses have the following methods:
#'
#' - `$Inspect()`: Walks the files in the directory and returns a common [Schema]
#' - `$Finish(schema)`: Returns a `DataSource`
#' @rdname DataSource
#' @name DataSource
#' @seealso [Dataset] for what do do with a `DataSource`
#' @export
DataSource <- R6Class("DataSource", inherit = Object,
  active = list(
    #' @description
    #' Return the DataSource's `Schema`
    schema = function() {
      shared_ptr(Schema, dataset___DataSource__schema(self))
    }
  )
)

#' @usage NULL
#' @format NULL
#' @rdname DataSource
#' @export
DataSourceDiscovery <- R6Class("DataSourceDiscovery", inherit = Object,
  public = list(
    Finish = function(schema = NULL) {
      if (is.null(schema)) {
        shared_ptr(DataSource, dataset___DSDiscovery__Finish1(self))
      } else {
        shared_ptr(DataSource, dataset___DSDiscovery__Finish2(self, schema))
      }
    },
    Inspect = function() shared_ptr(Schema, dataset___DSDiscovery__Inspect(self))
  )
)
DataSourceDiscovery$create <- function(path,
                                       filesystem = c("auto", "local"),
                                       format = c("parquet", "arrow", "ipc"),
                                       partition = NULL,
                                       allow_non_existent = FALSE,
                                       recursive = TRUE,
                                       ...) {
  if (!inherits(filesystem, "FileSystem")) {
    filesystem <- match.arg(filesystem)
    if (filesystem == "auto") {
      # When there are other FileSystems supported, detect e.g. S3 from path
      filesystem <- "local"
    }
    filesystem <- list(
      local = LocalFileSystem
      # We'll register other file systems here
    )[[filesystem]]$create(...)
  }
  selector <- FileSelector$create(
    path,
    allow_non_existent = allow_non_existent,
    recursive = recursive
  )

  format <- FileFormat$create(match.arg(format))

  if (!is.null(partition)) {
    if (inherits(partition, "Schema")) {
      partition <- SchemaPartitionScheme$create(partition)
    } else if (is.character(partition)) {
      # These are the column/field names, and we should autodetect their types
      partition <- SchemaPartitionSchemeDiscovery$create(partition)
    }
  }
  FileSystemDataSourceDiscovery$create(filesystem, selector, format, partition)
}

#' Create a DataSource for a Dataset
#'
#' @param path A string file path containing data files
#' @param filesystem A string identifier for the filesystem corresponding to
#' `path`. Currently only "local" is supported.
#' @param format A string identifier of the format of the files in `path`.
#' Currently supported options are "parquet", "arrow", and "ipc" (an alias for
#' the Arrow file format)
#' @param partition One of
#'   * A `Schema`, in which case the file paths relative to `sources` will be
#'    parsed, and path segments will be matched with the schema fields. For
#'    example, `schema(year = int16(), month = int8())` would create partitions
#'    for file paths like "2019/01/file.parquet", "2019/02/file.parquet", etc.
#'   * A character vector that defines the field names corresponding to those
#'    path segments (that is, you're providing the names that would correspond
#'    to a `Schema` but the types will be autodetected)
#'   * A `HivePartitionScheme` or `HivePartitionSchemeDiscovery`, as returned
#'    by [hive_partition()] which parses explicit or autodetected fields from
#'    Hive-style path segments
#'   * `NULL` for no partitioning
#' @param allow_non_existent logical: is `path` allowed to not exist? Default
#' `FALSE`. See [FileSelector].
#' @param recursive logical: should files be discovered in subdirectories of
#' `path`? Default `TRUE`.
#' @param ... Additional arguments passed to the [FileSystem] `$create()` method
#' @return A `DataSourceDiscovery` object. Pass this to [open_dataset()],
#' in a list potentially with other `DataSourceDiscovery` objects, to create
#' a `Dataset`.
#' @export
data_source <- DataSourceDiscovery$create

#' @usage NULL
#' @format NULL
#' @rdname DataSource
#' @export
FileSystemDataSourceDiscovery <- R6Class("FileSystemDataSourceDiscovery",
  inherit = DataSourceDiscovery
)
FileSystemDataSourceDiscovery$create <- function(filesystem,
                                                 selector,
                                                 format,
                                                 partition_scheme = NULL) {
  assert_is(filesystem, "FileSystem")
  assert_is(selector, "FileSelector")
  assert_is(format, "FileFormat")
  if (is.null(partition_scheme)) {
    shared_ptr(
      FileSystemDataSourceDiscovery,
      dataset___FSDSDiscovery__Make1(filesystem, selector, format)
    )
  } else if (inherits(partition_scheme, "PartitionSchemeDiscovery")) {
    shared_ptr(
      FileSystemDataSourceDiscovery,
      dataset___FSDSDiscovery__Make3(filesystem, selector, format, partition_scheme)
    )
  } else {
    assert_is(partition_scheme, "PartitionScheme")
    shared_ptr(
      FileSystemDataSourceDiscovery,
      dataset___FSDSDiscovery__Make2(filesystem, selector, format, partition_scheme)
    )
  }
}

FileFormat <- R6Class("FileFormat", inherit = Object)
FileFormat$create <- function(format, ...) {
  # TODO: pass list(...) options to the initializers
  # https://issues.apache.org/jira/browse/ARROW-7547
  if (format == "parquet") {
    shared_ptr(ParquetFileFormat, dataset___ParquetFileFormat__Make())
  } else if (format %in% c("ipc", "arrow")) { # These are aliases for the same thing
    shared_ptr(IpcFileFormat, dataset___IpcFileFormat__Make())
  } else {
    stop("Unsupported file format: ", format, call. = FALSE)
  }
}

ParquetFileFormat <- R6Class("ParquetFileFormat", inherit = FileFormat)
IpcFileFormat <- R6Class("IpcFileFormat", inherit = FileFormat)

#' Scan the contents of a dataset
#'
#' @description
#' A `Scanner` iterates over a [Dataset]'s data fragments and returns data
#' according to given row filtering and column projection. Use a
#' `ScannerBuilder`, from a `Dataset`'s `$NewScan()` method, to construct one.
#'
#' @section Methods:
#' `ScannerBuilder` has the following methods:
#'
#' - `$Project(cols)`: Indicate that the scan should only return columns given
#' by `cols`, a character vector of column names
#' - `$Filter(expr)`: Filter rows by an [Expression].
#' - `$UseThreads(threads)`: logical: should the scan use multithreading?
#' The method's default input is `TRUE`, but you must call the method to enable
#' multithreading because the scanner default is `FALSE`.
#' - `$schema`: Active binding, returns the [Schema] of the Dataset
#' - `$Finish()`: Returns a `Scanner`
#'
#' `Scanner` currently has a single method, `$ToTable()`, which evaluates the
#' query and returns an Arrow [Table].
#' @rdname Scanner
#' @name Scanner
#' @export
Scanner <- R6Class("Scanner", inherit = Object,
  public = list(
    ToTable = function() shared_ptr(Table, dataset___Scanner__ToTable(self))
  )
)

#' @usage NULL
#' @format NULL
#' @rdname Scanner
#' @export
ScannerBuilder <- R6Class("ScannerBuilder", inherit = Object,
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
    Finish = function() unique_ptr(Scanner, dataset___ScannerBuilder__Finish(self))
  ),
  active = list(
    schema = function() shared_ptr(Schema, dataset___ScannerBuilder__schema(self))
  )
)

#' @export
names.ScannerBuilder <- function(x) names(x$schema)

#' Define a partition scheme for a DataSource
#'
#' @description
#' Pass a `PartitionScheme` to a [FileSystemDataSourceDiscovery]'s `$create()`
#' method to indicate how the file's paths should be interpreted to define
#' partitioning.
#'
#' A `SchemaPartitionScheme` describes how to interpret raw path segments, in
#' order. For example, `schema(year = int16(), month = int8())` would define
#' partitions for file paths like "2019/01/file.parquet",
#' "2019/02/file.parquet", etc.
#'
#' A `HivePartitionScheme` is for Hive-style partitioning, which embeds field
#' names and values in path segments, such as
#' "/year=2019/month=2/data.parquet". Because fields are named in the path
#' segments, order does not matter.
#' @section Factory:
#' Both `SchemaPartitionScheme$create()` and `HivePartitionScheme$create()`
#' factory methods take a [Schema] as a single input argument. The helper
#' function `hive_partition(...)` is shorthand for
#' `HivePartitionScheme$create(schema(...))`.
#' @name PartitionScheme
#' @rdname PartitionScheme
#' @export
PartitionScheme <- R6Class("PartitionScheme", inherit = Object)
#' @usage NULL
#' @format NULL
#' @rdname PartitionScheme
#' @export
SchemaPartitionScheme <- R6Class("SchemaPartitionScheme", inherit = PartitionScheme)
SchemaPartitionScheme$create <- function(schema) {
  shared_ptr(SchemaPartitionScheme, dataset___SchemaPartitionScheme(schema))
}

#' @usage NULL
#' @format NULL
#' @rdname PartitionScheme
#' @export
HivePartitionScheme <- R6Class("HivePartitionScheme", inherit = PartitionScheme)
HivePartitionScheme$create <- function(schema) {
  shared_ptr(HivePartitionScheme, dataset___HivePartitionScheme(schema))
}

#' Construct a Hive partition scheme
#'
#' Hive partitioning embeds field names and values in path segments, such as
#' "/year=2019/month=2/data.parquet". A [HivePartitionScheme][PartitionScheme]
#' is used to parse that in Dataset creation.
#'
#' Because fields are named in the path segments, order of fields passed to
#' `hive_partition()` does not matter.
#' @param ... named list of [data types][data-type], passed to [schema()]
#' @return A `HivePartitionScheme`, or a `HivePartitionSchemeDiscovery` if
#' calling `hive_partition()` with no arguments.
#' @examples
#' \donttest{
#' hive_partition(year = int16(), month = int8())
#' }
#' @export
hive_partition <- function(...) {
  schm <- schema(...)
  if (length(schm) == 0) {
    HivePartitionSchemeDiscovery$create()
  } else {
    HivePartitionScheme$create(schm)
  }
}

PartitionSchemeDiscovery <- R6Class("PartitionSchemeDiscovery", inherit = Object)
SchemaPartitionSchemeDiscovery <- R6Class("SchemaPartitionSchemeDiscovery", inherit = PartitionSchemeDiscovery)
SchemaPartitionSchemeDiscovery$create <- function(x) {
  shared_ptr(SchemaPartitionSchemeDiscovery, dataset___SchemaPartitionScheme__MakeDiscovery(x))
}

HivePartitionSchemeDiscovery <- R6Class("HivePartitionSchemeDiscovery", inherit = PartitionSchemeDiscovery)
HivePartitionSchemeDiscovery$create <- function() {
  shared_ptr(HivePartitionSchemeDiscovery, dataset___HivePartitionScheme__MakeDiscovery())
}
