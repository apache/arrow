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
#' @param sources Either a string path to a directory containing data files,
#' or a list of `SourceFactory` objects as created by [open_source()].
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
#' @param ... additional arguments passed to `open_source()` when `sources` is
#' a file path, otherwise ignored.
#' @return A [Dataset] R6 object. Use `dplyr` methods on it to query the data,
#' or call [`$NewScan()`][Scanner] to construct a query directly.
#' @export
#' @seealso `vignette("dataset", package = "arrow")`
#' @include arrow-package.R
open_dataset <- function(sources, schema = NULL, partitioning = hive_partition(), ...) {
  if (is.character(sources)) {
    sources <- list(open_source(sources, partitioning = partitioning, ...))
  }
  DatasetFactory$create(sources)$Finish(schema)
}

#' Multi-file datasets
#'
#' @description
#' Arrow Datasets allow you to query against data that has been split across
#' multiple files. This sharding of data may indicate partitioning, which
#' can accelerate queries that only touch some partitions (files).
#'
#' `DatasetFactory` is used to help in the creation of `Dataset`s.
#' @section Factory:
#' The `Dataset$create()` method instantiates a `Dataset` and
#' takes the following arguments:
#' * `sources`: a list of [Source] objects
#' * `schema`: a [Schema]
#'
#' The `DatasetFactory$create()` takes the following arguments:
#' * `sources`: a list of [SourceFactory] objects
#' @section Methods:
#'
#' A `Dataset` has the following methods:
#' - `$NewScan()`: Returns a [ScannerBuilder] for building a query
#' - `$schema`: Active binding, returns the [Schema] of the Dataset
#'
#' A `DatasetFactory` has:
#'
#' - `$Inspect()`: Returns a common [Schema] for the `Sources` in the factory.
#' - `$Finish(schema)`: Returns a `Dataset`
#' @export
#' @seealso [open_dataset()] for a simple interface to creating a `Dataset`
Dataset <- R6Class("Dataset", inherit = Object,
  public = list(
    #' @description
    #' Start a new scan of the data
    #' @return A [ScannerBuilder]
    NewScan = function() unique_ptr(ScannerBuilder, dataset___Dataset__NewScan(self)),
    ToString = function() self$schema$ToString()
  ),
  active = list(
    #' @description
    #' Return the Dataset's `Schema`
    schema = function() shared_ptr(Schema, dataset___Dataset__schema(self)),
    metadata = function() self$schema$metadata,
    #' @description
    #' Return the Dataset's `Source`s
    sources = function() {
      map(dataset___Dataset__sources(self), ~shared_ptr(Source, .)$..dispatch())
    }
  )
)
Dataset$create <- function(sources, schema) {
  # TODO: consider deleting Dataset$create since we have DatasetFactory$create
  assert_is_list_of(sources, "Source")
  assert_is(schema, "Schema")
  shared_ptr(Dataset, dataset___Dataset__create(sources, schema))
}

#' @export
names.Dataset <- function(x) names(x$schema)

#' @usage NULL
#' @format NULL
#' @rdname Dataset
#' @export
DatasetFactory <- R6Class("DatasetFactory", inherit = Object,
  public = list(
    Finish = function(schema = NULL) {
      if (is.null(schema)) {
        shared_ptr(Dataset, dataset___DFactory__Finish1(self))
      } else {
        assert_is(schema, "Schema")
        shared_ptr(Dataset, dataset___DFactory__Finish2(self, schema))
      }
    },
    Inspect = function() shared_ptr(Schema, dataset___DFactory__Inspect(self))
  )
)
DatasetFactory$create <- function(sources) {
  assert_is_list_of(sources, "SourceFactory")
  shared_ptr(DatasetFactory, dataset___DFactory__Make(sources))
}


#' Sources for a Dataset
#'
#' @description
#' A [Dataset] can have one or more `Source`s. A `Source` contains one or more
#' `Fragments`, such as files, of a common type and partitioning.
#' `SourceFactory` is used to create a `Source`, inspect the [Schema] of the
#' fragments contained in it, and declare a partitioning.
#' `FileSystemSourceFactory` is a subclass of `SourceFactory` for
#' discovering files in the local file system, the only currently supported
#' file system, it constructs an instance of `FileSystemSource`.
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
#' * `format`: A string identifier of the format of the files in `path`.
#'   Currently supported options are "parquet", "arrow", and "ipc" (an alias for
#'   the Arrow file format)
#' @section Methods:
#' `Source` and its subclasses have the following method:
#'
#' - `$schema`: Active binding, returns the [Schema] of the `Source`
#'
#' `FileSystemSource` has the following methods:
#'
#' - `$files`: Active binding, returns the files of the `FileSystemSource`
#' - `$format`: Active binding, returns the [FileFormat] of the `FileSystemSource`
#'
#' `SourceFactory` and its subclasses have the following methods:
#'
#' - `$Inspect()`: Walks the files in the directory and returns a common [Schema]
#' - `$Finish(schema)`: Returns a `Source`
#' @rdname Source
#' @name Source
#' @seealso [Dataset] for what to do with a `Source`
#' @export
Source <- R6Class("Source", inherit = Object,
  public = list(
    ..dispatch = function() {
      if (self$type == "filesystem") {
        shared_ptr(FileSystemSource, self$pointer())
      } else {
        self
      }
    }
  ),
  active = list(
    #' @description
    #' Return the Source's `Schema`
    schema = function() {
      shared_ptr(Schema, dataset___Source__schema(self))
    },
    #' @description
    #' Return the Source's type.
    type = function() dataset___Source__type_name(self)
  )
)

#' @name FileSystemSource
#' @rdname Source
#' @export
FileSystemSource <- R6Class("FileSystemSource", inherit = Source,
  active = list(
    #' @description
    #' Return the files contained in this `Source`
    files = function() dataset___FSSource__files(self),
    #' @description
    #' Return the format of files in this `Source`
    format = function() {
      shared_ptr(FileFormat, dataset___FSSource__format(self))$..dispatch()
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
        ptr <- dataset___SFactory__Finish1(self)
      } else {
        ptr <- dataset___SFactory__Finish2(self, schema)
      }
      shared_ptr(Source, ptr)$..dispatch()
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
#' A [Dataset] can have one or more [Source]s. A `Source` contains one or more
#' `Fragments`, such as files, of a common storage location, format, and
#' partitioning. This function helps you construct a `Source` that you can
#' pass to [open_dataset()].
#'
#' If you only have a single `Source`, such as a directory containing Parquet
#' files, you can call `open_dataset()` directly. Use `open_source()` when you
#' want to combine different directories, file systems, or file formats.
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
    ptr <- dataset___FSSFactory__Make1(filesystem, selector, format)
  } else if (inherits(partitioning, "PartitioningFactory")) {
    ptr <- dataset___FSSFactory__Make3(filesystem, selector, format, partitioning)
  } else if (inherits(partitioning, "Partitioning")) {
    ptr <- dataset___FSSFactory__Make2(filesystem, selector, format, partitioning)
  } else {
    stop(
      "Expected 'partitioning' to be NULL, PartitioningFactory or Partitioning",
      call. = FALSE
    )
  }

  shared_ptr(FileSystemSourceFactory, ptr)
}

#' Dataset file formats
#'
#' @description
#' A `FileFormat` holds information about how to read and parse the files
#' included in a `Source`. There are subclasses corresponding to the supported
#' file formats (`ParquetFileFormat` and `IpcFileFormat`).
#'
#' @section Factory:
#' `FileFormat$create()` takes the following arguments:
#' * `format`: A string identifier of the format of the files in `path`.
#'   Currently supported options are "parquet", "arrow", and "ipc" (an alias for
#'   the Arrow file format)
#' * `...`: Additional format-specific options
#'
#' It returns the appropriate subclass of `FileFormat` (e.g. `ParquetFileFormat`)
#' @rdname FileFormat
#' @name FileFormat
#' @export
FileFormat <- R6Class("FileFormat", inherit = Object,
  public = list(
    ..dispatch = function() {
      type <- self$type
      if (type == "parquet") {
        shared_ptr(ParquetFileFormat, self$pointer())
      } else if (type == "ipc") {
        shared_ptr(IpcFileFormat, self$pointer())
      } else {
        self
      }
    }
  ),
  active = list(
    #' @description
    #' Return the `FileFormat`'s type
    type = function() dataset___FileFormat__type_name(self)
  )
)
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

#' @usage NULL
#' @format NULL
#' @rdname FileFormat
#' @export
ParquetFileFormat <- R6Class("ParquetFileFormat", inherit = FileFormat)

#' @usage NULL
#' @format NULL
#' @rdname FileFormat
#' @export
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

#' Define Partitioning for a Source
#'
#' @description
#' Pass a `Partitioning` object to a [FileSystemSourceFactory]'s `$create()`
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
#' `PartitioningFactory` subclasses instruct the `SourceFactory` to detect
#' partition features from the file paths.
#' @section Factory:
#' Both `DirectoryPartitioning$create()` and `HivePartitioning$create()`
#' methods take a [Schema] as a single input argument. The helper
#' function [`hive_partition(...)`][hive_partition] is shorthand for
#' `HivePartitioning$create(schema(...))`.
#'
#' With `DirectoryPartitioningFactory$create()`, you can provide just the
#' names of the path segments (in our example, `c("year", "month")`), and
#' the `SourceFactory` will infer the data types for those partition variables.
#' `HivePartitioningFactory$create()` takes no arguments: both variable names
#' and their types can be inferred from the file paths. `hive_partition()` with
#' no arguments returns a `HivePartitioningFactory`.
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

PartitioningFactory <- R6Class("PartitioningFactory", inherit = Object)

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
