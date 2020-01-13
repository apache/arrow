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
#' or (2) a list of `SourceFactory` objects as created by [open_source()].
#' @param schema [Schema] for the dataset. If `NULL` (the default), the schema
#' will be inferred from the data sources.
#' @param partitioning When `sources` is a file path, one of
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
#' @param ... additional arguments passed to `open_source()`, when `sources` is
#' a file path, otherwise ignored.
#' @return A [Dataset] R6 object. Use `dplyr` methods on it to query the data,
#' or call `$NewScan()` to construct a query directly.
#' @export
#' @seealso [Partitioning] for defining partitions
#' @include arrow-package.R
open_dataset <- function(sources, schema = NULL, partitioning = hive_partition(), ...) {
  if (is.character(sources)) {
    sources <- list(open_source(sources, partitioning = partitioning, ...))
  }
  assert_is_list_of(sources, "SourceFactory")
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
#' * `sources`: a list of [Source] objects
#' * `schema`: a [Schema]
#' @section Methods:
#'
#' - `$NewScan()`: Returns a [ScannerBuilder] for building a query
#' - `$schema`: Active binding, returns the [Schema] of the Dataset
#' @export
#' @seealso [open_dataset()] for a simple way to create a Dataset that has a
#' single `Source`.
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
  assert_is_list_of(sources, "Source")
  assert_is(schema, "Schema")
  shared_ptr(Dataset, dataset___Dataset__create(sources, schema))
}

#' @export
names.Dataset <- function(x) names(x$schema)

#' Sources for a Dataset
#'
#' @description
#' A [Dataset] can have one or more `Source`s. A `Source` contains one or more
#' `Fragments`, such as files, of a common type and partitioning.
#' `SourceFactory` is used to create a `Source`, inspect the [Schema] of the
#' fragments contained in it, and declare a partitioning.
#' `FileSystemSourceFactory` is a subclass of `SourceFactory` for
#' discovering files in the local file system, the only currently supported
#' file system.
#'
#' In general, you'll deal with `SourceFactory` rather than `Source` itself.
#' @section Factory:
#' For the `SourceFactory$create()` factory method, see [open_source()], an
#' alias for it.
#'
#' `FileSystemSourceFactory$create()` is a lower-level factory method and
#' takes the following arguments:
#' * `filesystem`: A [FileSystem]
#' * `selector`: A [FileSelector]
#' * `format`: Currently only "parquet" is supported
#' @section Methods:
#' `Source` has one defined method:
#'
#' - `$schema`: Active binding, returns the [Schema] of the `Source`
#'
#' `SourceFactory` and its subclasses have the following methods:
#'
#' - `$Inspect()`: Walks the files in the directory and returns a common [Schema]
#' - `$Finish(schema)`: Returns a `Source`
#' @rdname Source
#' @name Source
#' @seealso [Dataset] for what do do with a `Source`
#' @export
Source <- R6Class("Source", inherit = Object,
  active = list(
    #' @description
    #' Return the Source's `Schema`
    schema = function() {
      shared_ptr(Schema, dataset___Source__schema(self))
    }
  )
)

#' @usage NULL
#' @format NULL
#' @rdname Source
#' @export
SourceFactory <- R6Class("SourceFactory", inherit = Object,
  public = list(
    Finish = function(schema = NULL) {
      if (is.null(schema)) {
        shared_ptr(Source, dataset___SFactory__Finish1(self))
      } else {
        shared_ptr(Source, dataset___SFactory__Finish2(self, schema))
      }
    },
    Inspect = function() shared_ptr(Schema, dataset___SFactory__Inspect(self))
  )
)
SourceFactory$create <- function(path,
                                 filesystem = c("auto", "local"),
                                 format = c("parquet", "arrow", "ipc"),
                                 partitioning = NULL,
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

  if (!is.null(partitioning)) {
    if (inherits(partitioning, "Schema")) {
      partitioning <- DirectoryPartitioning$create(partitioning)
    } else if (is.character(partitioning)) {
      # These are the column/field names, and we should autodetect their types
      partitioning <- DirectoryPartitioningFactory$create(partitioning)
    }
  }
  FileSystemSourceFactory$create(filesystem, selector, format, partitioning)
}

#' Create a Source for a Dataset
#'
#' @param path A string file path containing data files
#' @param filesystem A string identifier for the filesystem corresponding to
#' `path`. Currently only "local" is supported.
#' @param format A string identifier of the format of the files in `path`.
#' Currently supported options are "parquet", "arrow", and "ipc" (an alias for
#' the Arrow file format)
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
#' @param allow_non_existent logical: is `path` allowed to not exist? Default
#' `FALSE`. See [FileSelector].
#' @param recursive logical: should files be discovered in subdirectories of
#' `path`? Default `TRUE`.
#' @param ... Additional arguments passed to the [FileSystem] `$create()` method
#' @return A `SourceFactory` object. Pass this to [open_dataset()],
#' in a list potentially with other `SourceFactory` objects, to create
#' a `Dataset`.
#' @export
open_source <- SourceFactory$create

#' @usage NULL
#' @format NULL
#' @rdname Source
#' @export
FileSystemSourceFactory <- R6Class("FileSystemSourceFactory",
  inherit = SourceFactory
)
FileSystemSourceFactory$create <- function(filesystem,
                                           selector,
                                           format,
                                           partitioning = NULL) {
  assert_is(filesystem, "FileSystem")
  assert_is(selector, "FileSelector")
  assert_is(format, "FileFormat")
  if (is.null(partitioning)) {
    shared_ptr(
      FileSystemSourceFactory,
      dataset___FSSFactory__Make1(filesystem, selector, format)
    )
  } else if (inherits(partitioning, "PartitioningFactory")) {
    shared_ptr(
      FileSystemSourceFactory,
      dataset___FSSFactory__Make3(filesystem, selector, format, partitioning)
    )
  } else {
    assert_is(partitioning, "Partitioning")
    shared_ptr(
      FileSystemSourceFactory,
      dataset___FSSFactory__Make2(filesystem, selector, format, partitioning)
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
#' A `Scanner` iterates over a [Dataset]'s fragments and returns data
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

#' Define a partitioning for a Source
#'
#' @description
#' Pass a `Partitioning` to a [FileSystemSourceFactory]'s `$create()`
#' method to indicate how the file's paths should be interpreted to define
#' partitioning.
#'
#' A `DirectoryPartitioning` describes how to interpret raw path segments, in
#' order. For example, `schema(year = int16(), month = int8())` would define
#' partitions for file paths like "2019/01/file.parquet",
#' "2019/02/file.parquet", etc.
#'
#' A `HivePartitioning` is for Hive-style partitioning, which embeds field
#' names and values in path segments, such as
#' "/year=2019/month=2/data.parquet". Because fields are named in the path
#' segments, order does not matter.
#' @section Factory:
#' Both `DirectoryPartitioning$create()` and `HivePartitioning$create()`
#' factory methods take a [Schema] as a single input argument. The helper
#' function `hive_partition(...)` is shorthand for
#' `HivePartitioning$create(schema(...))`.
#' @name Partitioning
#' @rdname Partitioning
#' @export
Partitioning <- R6Class("Partitioning", inherit = Object)
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

#' Construct a Hive partitioning
#'
#' Hive partitioning embeds field names and values in path segments, such as
#' "/year=2019/month=2/data.parquet". A [HivePartitioning][Partitioning]
#' is used to parse that in Dataset creation.
#'
#' Because fields are named in the path segments, order of fields passed to
#' `hive_partition()` does not matter.
#' @param ... named list of [data types][data-type], passed to [schema()]
#' @return A `HivePartitioning`, or a `HivePartitioningFactory` if
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

PartitioningFactory <- R6Class("PartitioningFactory", inherit = Object)
DirectoryPartitioningFactory <- R6Class("DirectoryPartitioningFactory ", inherit = PartitioningFactory)
DirectoryPartitioningFactory$create <- function(x) {
  shared_ptr(DirectoryPartitioningFactory, dataset___DirectoryPartitioning__MakeFactory(x))
}

HivePartitioningFactory <- R6Class("HivePartitioningFactory", inherit = PartitioningFactory)
HivePartitioningFactory$create <- function() {
  shared_ptr(HivePartitioningFactory, dataset___HivePartitioning__MakeFactory())
}
