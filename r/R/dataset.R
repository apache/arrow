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
#' - `$write(path, filesystem, schema, format, partitioning, ...)`: writes the
#'   dataset to `path` in the `format` file format, partitioned by `partitioning`,
#'   and invisibly returns `self`. See [write_dataset()].
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
    ToString = function() self$schema$ToString(),
    write = function(path, filesystem = NULL, schema = self$schema, format, partitioning, ...) {
      path_and_fs <- get_path_and_filesystem(path, filesystem)
      dataset___Dataset__Write(self, schema, format, path_and_fs$fs, path_and_fs$path, partitioning)
      invisible(self)
    }
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
    # @description
    # Return the filesystem of files in this `Dataset`
    filesystem = function() {
      shared_ptr(FileSystem, dataset___FileSystemDataset__filesystem(self))$..dispatch()
    },
    num_rows = function() {
      if (inherits(self$format, "ParquetFileFormat")) {
        # It's generally fast enough to skim the files directly
        sum(map_int(self$files, ~ParquetFileReader$create(.x)$num_rows))
      } else {
        # TODO: implement for other file formats
        warning("Number of rows unknown; returning NA", call. = FALSE)
        NA_integer_
        # Could do a scan, picking only the last column, which hopefully is virtual
        # But this is can be slow
        # Scanner$create(self, projection = tail(names(self), 1))$ToTable()$num_rows
        # See also https://issues.apache.org/jira/browse/ARROW-9697
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

#' @name InMemoryDataset
#' @rdname Dataset
#' @export
InMemoryDataset <- R6Class("InMemoryDataset", inherit = Dataset)
InMemoryDataset$create <- function(x) {
  if (!inherits(x, "Table")) {
    x <- Table$create(x)
  }
  shared_ptr(InMemoryDataset, dataset___InMemoryDataset__create(x))
}


#' @export
names.Dataset <- function(x) names(x$schema)

#' @export
dim.Dataset <- function(x) c(x$num_rows, x$num_cols)

#' @export
c.Dataset <- function(...) Dataset$create(list(...))

#' @export
head.Dataset <- function(x, n = 6L, ...) {
  assert_that(n > 0) # For now
  scanner <- Scanner$create(ensure_group_vars(x))
  shared_ptr(Table, dataset___Scanner__head(scanner, n))
}

#' @export
tail.Dataset <- function(x, n = 6L, ...) {
  assert_that(n > 0) # For now
  result <- list()
  batch_num <- 0
  scanner <- Scanner$create(ensure_group_vars(x))
  for (scan_task in rev(dataset___Scanner__Scan(scanner))) {
    for (batch in rev(shared_ptr(ScanTask, scan_task)$Execute())) {
      batch_num <- batch_num + 1
      result[[batch_num]] <- tail(batch, n)
      n <- n - nrow(batch)
      if (n <= 0) break
    }
    if (n <= 0) break
  }
  Table$create(!!!rev(result))
}

#' @export
`[.Dataset` <- function(x, i, j, ..., drop = FALSE) {
  if (nargs() == 2L) {
    # List-like column extraction (x[i])
    return(x[, i])
  }
  if (!missing(j)) {
    x <- select.Dataset(x, j)
  }

  if (!missing(i)) {
    x <- take_dataset_rows(x, i)
  }
  x
}

take_dataset_rows <- function(x, i) {
  # TODO: move this to cpp
  if (!is.numeric(i) || any(i < 0)) {
    stop("Only slicing with positive indices is supported", call. = FALSE)
  }
  result <- list()
  result_order <- order(i)
  i <- sort(i) - 1L
  scanner <- Scanner$create(ensure_group_vars(x))
  for (scan_task in dataset___Scanner__Scan(scanner)) {
    for (batch in shared_ptr(ScanTask, scan_task)$Execute()) {
      # Take all rows that are in this batch
      this_batch_nrows <- batch$num_rows
      in_this_batch <- i > -1L & i < this_batch_nrows
      if (any(in_this_batch)) {
        result[[length(result) + 1L]] <- batch$Take(i[in_this_batch])
      }
      i <- i - this_batch_nrows
      if (all(i < 0L)) break
    }
    if (all(i < 0L)) break
  }
  tab <- Table$create(!!!result)
  # Now sort
  tab$Take(result_order - 1L)
}
