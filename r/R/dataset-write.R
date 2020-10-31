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

#' Write a dataset
#'
#' This function allows you to write a dataset. By writing to more efficient
#' binary storage formats, and by specifying relevant partitioning, you can
#' make it much faster to read and query.
#'
#' @param dataset [Dataset], [RecordBatch], [Table], `arrow_dplyr_query`, or
#' `data.frame`. If an `arrow_dplyr_query` or `grouped_df`,
#' `schema` and `partitioning` will be taken from the result of any `select()`
#' and `group_by()` operations done on the dataset. `filter()` queries will be
#' applied to restrict written rows.
#' Note that `select()`-ed columns may not be renamed.
#' @param path string path, URI, or `SubTreeFileSystem` referencing a directory
#' to write to (directory will be created if it does not exist)
#' @param format file format to write the dataset to. Currently supported
#' formats are "feather" (aka "ipc") and "parquet". Default is to write to the
#' same format as `dataset`.
#' @param partitioning `Partitioning` or a character vector of columns to
#' use as partition keys (to be written as path segments). Default is to
#' use the current `group_by()` columns.
#' @param basename_template string template for the names of files to be written.
#' Must contain `"{i}"`, which will be replaced with an autoincremented
#' integer to generate basenames of datafiles. For example, `"part-{i}.feather"`
#' will yield `"part-0.feather", ...`.
#' @param hive_style logical: write partition segments as Hive-style
#' (`key1=value1/key2=value2/file.ext`) or as just bare values. Default is `TRUE`.
#' @param ... additional format-specific arguments. For available Parquet
#' options, see [write_parquet()]. The available Feather options are
#' - `use_legacy_format` logical: write data formatted so that Arrow libraries
#'   versions 0.14 and lower can read it. Default is `FALSE`. You can also
#'   enable this by setting the environment variable `ARROW_PRE_0_15_IPC_FORMAT=1`.
#' - `metadata_version`: A string like "V5" or the equivalent integer indicating
#'   the Arrow IPC MetadataVersion. Default (NULL) will use the latest version,
#'   unless the environment variable `ARROW_PRE_1_0_METADATA_VERSION=1`, in
#'   which case it will be V4.
#' - `codec`: A [Codec] which will be used to compress body buffers of written
#'   files. Default (NULL) will not compress body buffers.
#' @return The input `dataset`, invisibly
#' @export
write_dataset <- function(dataset,
                          path,
                          format = dataset$format,
                          partitioning = dplyr::group_vars(dataset),
                          basename_template = paste0("part-{i}.", as.character(format)),
                          hive_style = TRUE,
                          ...) {
  if (inherits(dataset, "arrow_dplyr_query")) {
    # We can select a subset of columns but we can't rename them
    if (!all(dataset$selected_columns == names(dataset$selected_columns))) {
      stop("Renaming columns when writing a dataset is not yet supported", call. = FALSE)
    }
    # partitioning vars need to be in the `select` schema
    dataset <- ensure_group_vars(dataset)
  } else if (inherits(dataset, "grouped_df")) {
    force(partitioning)
    # Drop the grouping metadata before writing; we've already consumed it
    # now to construct `partitioning` and don't want it in the metadata$r
    dataset <- dplyr::ungroup(dataset)
  }

  scanner <- Scanner$create(dataset)
  if (!inherits(partitioning, "Partitioning")) {
    partition_schema <- scanner$schema[partitioning]
    if (isTRUE(hive_style)) {
      partitioning <- HivePartitioning$create(partition_schema)
    } else {
      partitioning <- DirectoryPartitioning$create(partition_schema)
    }
  }

  path_and_fs <- get_path_and_filesystem(path)
  options <- FileWriteOptions$create(format, table = scanner, ...)

  dataset___Dataset__Write(options, path_and_fs$fs, path_and_fs$path,
                           partitioning, basename_template, scanner)
}
