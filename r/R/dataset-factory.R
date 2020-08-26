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

#' @include dataset.R

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

  path_and_fs <- get_path_and_filesystem(x, filesystem)
  selector <- FileSelector$create(path_and_fs$path, allow_not_found = FALSE, recursive = TRUE)

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
  FileSystemDatasetFactory$create(path_and_fs$fs, selector, format, partitioning)
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
