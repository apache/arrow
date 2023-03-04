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
DatasetFactory <- R6Class("DatasetFactory",
  inherit = ArrowObject,
  public = list(
    Finish = function(schema = NULL, unify_schemas = FALSE) {
      if (is.null(schema)) {
        dataset___DatasetFactory__Finish1(self, unify_schemas)
      } else {
        assert_is(schema, "Schema")
        dataset___DatasetFactory__Finish2(self, schema)
      }
    },
    Inspect = function(unify_schemas = FALSE) {
      dataset___DatasetFactory__Inspect(self, unify_schemas)
    }
  )
)
DatasetFactory$create <- function(x,
                                  filesystem = NULL,
                                  format = c("parquet", "arrow", "ipc", "feather", "csv", "tsv", "text"),
                                  partitioning = NULL,
                                  hive_style = NA,
                                  factory_options = list(),
                                  ...) {
  if (is_list_of(x, "DatasetFactory")) {
    return(dataset___UnionDatasetFactory__Make(x))
  }

  if (is.character(format)) {
    format <- FileFormat$create(match.arg(format), ...)
  } else {
    assert_is(format, "FileFormat")
  }

  path_and_fs <- get_paths_and_filesystem(x, filesystem)
  info <- path_and_fs$fs$GetFileInfo(path_and_fs$path)

  if (length(info) > 1 || info[[1]]$type == FileType$File) {
    # x looks like a vector of one or more file paths (not a directory path)
    return(FileSystemDatasetFactory$create(
      path_and_fs$fs,
      NULL,
      path_and_fs$path,
      format,
      factory_options = factory_options
    ))
  }

  partitioning <- handle_partitioning(partitioning, path_and_fs, hive_style)
  selector <- FileSelector$create(
    path_and_fs$path,
    allow_not_found = FALSE,
    recursive = TRUE
  )

  FileSystemDatasetFactory$create(path_and_fs$fs, selector, NULL, format, partitioning, factory_options)
}

handle_partitioning <- function(partitioning, path_and_fs, hive_style) {
  # Handle partitioning arg in cases where it is "character" or "Schema"
  if (!is.null(partitioning) && !inherits(partitioning, c("Partitioning", "PartitioningFactory"))) {
    if (!is_false(hive_style)) {
      # Default is NA, which means check to see if the paths could be hive_style
      hive_factory <- HivePartitioningFactory$create()
      paths <- path_and_fs$fs$ls(
        path_and_fs$path,
        allow_not_found = FALSE,
        recursive = TRUE
      )
      hive_schema <- hive_factory$Inspect(paths)
      # This is length-0 if there are no hive segments
      if (is.na(hive_style)) {
        hive_style <- length(hive_schema) > 0
      }
    }

    if (hive_style) {
      if (is.character(partitioning)) {
        # These are not needed, the user probably provided them because they
        # thought they needed to. Just make sure they aren't invalid.
        if (!identical(names(hive_schema), partitioning)) {
          abort(c(
            paste(
              '"partitioning" does not match the detected Hive-style partitions:',
              deparse1(names(hive_schema))
            ),
            i = 'Omit "partitioning" to use the Hive partitions',
            i = "Set `hive_style = FALSE` to override what was detected",
            i = "Or, to rename partition columns, call `select()` or `rename()` after opening the dataset"
          ))
        }
        partitioning <- hive_factory$Finish(hive_schema)
      } else if (inherits(partitioning, "Schema")) {
        # This means we want to set the types of the hive-style partitions
        # to be exactly what we want them to be
        if (!identical(names(hive_schema), names(partitioning))) {
          abort(c(
            paste(
              '"partitioning" does not match the detected Hive-style partitions:',
              deparse1(names(hive_schema))
            ),
            i = 'Omit "partitioning" to use the Hive partitions',
            i = "Set `hive_style = FALSE` to override what was detected",
            i = "Or, to rename partition columns, call `select()` or `rename()` after opening the dataset"
          ))
        }
        partitioning <- HivePartitioning$create(partitioning)
      }
    } else {
      # DirectoryPartitioning
      if (is.character(partitioning)) {
        # These are the column/field names, and we should autodetect their types
        partitioning <- DirectoryPartitioningFactory$create(partitioning)
      } else if (inherits(partitioning, "Schema")) {
        partitioning <- DirectoryPartitioning$create(partitioning)
      }
    }
  }
  partitioning
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
#' @param x A string path to a directory containing data files, a vector of one
#' one or more string paths to data files, or a list of `DatasetFactory` objects
#' whose datasets should be combined. If this argument is specified it will be
#' used to construct a `UnionDatasetFactory` and other arguments will be
#' ignored.
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
#'     parsed, and path segments will be matched with the schema fields. For
#'     example, `schema(year = int16(), month = int8())` would create partitions
#'     for file paths like "2019/01/file.parquet", "2019/02/file.parquet", etc.
#'   * A character vector that defines the field names corresponding to those
#'     path segments (that is, you're providing the names that would correspond
#'     to a `Schema` but the types will be autodetected)
#'   * A `HivePartitioning` or `HivePartitioningFactory`, as returned
#'     by [hive_partition()] which parses explicit or autodetected fields from
#'     Hive-style path segments
#'   * `NULL` for no partitioning
#' @param hive_style Logical: if `partitioning` is a character vector or a
#' `Schema`, should it be interpreted as specifying Hive-style partitioning?
#' Default is `NA`, which means to inspect the file paths for Hive-style
#' partitioning and behave accordingly.
#' @param factory_options list of optional FileSystemFactoryOptions:
#'   * `partition_base_dir`: string path segment prefix to ignore when
#'     discovering partition information with DirectoryPartitioning. Not
#'     meaningful (ignored with a warning) for HivePartitioning, nor is it
#'     valid when providing a vector of file paths.
#'   * `exclude_invalid_files`: logical: should files that are not valid data
#'     files be excluded? Default is `FALSE` because checking all files up
#'     front incurs I/O and thus will be slower, especially on remote
#'     filesystems. If false and there are invalid files, there will be an
#'     error at scan time. This is the only FileSystemFactoryOption that is
#'     valid for both when providing a directory path in which to discover
#'     files and when providing a vector of file paths.
#'   * `selector_ignore_prefixes`: character vector of file prefixes to ignore
#'     when discovering files in a directory. If invalid files can be excluded
#'     by a common filename prefix this way, you can avoid the I/O cost of
#'     `exclude_invalid_files`. Not valid when providing a vector of file paths
#'     (but if you're providing the file list, you can filter invalid files
#'     yourself).
#' @param ... Additional format-specific options, passed to
#' [`FileFormat$create()`][FileFormat]. For CSV options, note that you can specify them either
#' with the Arrow C++ library naming ("delimiter", "quoting", etc.) or the
#' `readr`-style naming used in [read_csv_arrow()] ("delim", "quote", etc.).
#' Not all `readr` options are currently supported; please file an issue if you
#' encounter one that `arrow` should support.
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
                                            selector = NULL,
                                            paths = NULL,
                                            format,
                                            partitioning = NULL,
                                            factory_options = list()) {
  assert_is(filesystem, "FileSystem")
  is.null(selector) || assert_is(selector, "FileSelector")
  is.null(paths) || assert_is(paths, "character")
  assert_that(
    xor(is.null(selector), is.null(paths)),
    msg = "Either selector or paths must be specified"
  )
  assert_is(format, "FileFormat")
  if (!is.null(paths)) {
    assert_that(
      is.null(partitioning),
      msg = "Partitioning not supported with paths"
    )
    # Validate that exclude_invalid_files is only option provided
    # All other options are only relevant for the FileSelector method
    invalid_opts <- setdiff(names(factory_options), "exclude_invalid_files")
    if (length(invalid_opts)) {
      stop(
        "Invalid factory_options for creating a Dataset from a vector of file paths: ",
        oxford_paste(invalid_opts),
        call. = FALSE
      )
    }
    return(dataset___FileSystemDatasetFactory__MakePaths(
      filesystem,
      paths,
      format,
      isTRUE(factory_options[["exclude_invalid_files"]])
    ))
  }

  dataset___FileSystemDatasetFactory__Make(
    filesystem,
    selector,
    format,
    fsf_options(factory_options, partitioning)
  )
}

fsf_options <- function(factory_options, partitioning) {
  # Validate FileSystemFactoryOptions and put partitioning in it
  valid_opts <- c(
    "partition_base_dir",
    "exclude_invalid_files",
    "selector_ignore_prefixes"
  )
  invalid_opts <- setdiff(names(factory_options), valid_opts)
  if (length(invalid_opts)) {
    stop("Invalid factory_options: ", oxford_paste(invalid_opts), call. = FALSE)
  }
  if (!is.null(factory_options$partition_base_dir)) {
    if (
      inherits(partitioning, "HivePartitioning") ||
        (
          inherits(partitioning, "PartitioningFactory") &&
            identical(partitioning$type_name, "hive")
        )
    ) {
      warning(
        "factory_options$partition_base_dir is not meaningful for Hive partitioning",
        call. = FALSE
      )
    } else {
      assert_that(is.string(factory_options$partition_base_dir))
    }
  }

  exclude <- factory_options$exclude_invalid_files %||% FALSE
  if (!(isTRUE(exclude) || is_false(exclude))) {
    stop(
      "factory_options$exclude_invalid_files must be TRUE/FALSE",
      call. = FALSE
    )
  }

  if (!is.character(factory_options$selector_ignore_prefixes %||% character())) {
    stop(
      "factory_options$selector_ignore_prefixes must be a character vector",
      call. = FALSE
    )
  }

  if (inherits(partitioning, "PartitioningFactory")) {
    factory_options[["partitioning_factory"]] <- partitioning
  } else if (inherits(partitioning, "Partitioning")) {
    factory_options[["partitioning"]] <- partitioning
  } else if (!is.null(partitioning)) {
    stop(
      "Expected 'partitioning' to be NULL, PartitioningFactory or Partitioning",
      call. = FALSE
    )
  }

  factory_options
}
