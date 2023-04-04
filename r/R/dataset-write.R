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
#' `data.frame`. If an `arrow_dplyr_query`, the query will be evaluated and
#' the result will be written. This means that you can `select()`, `filter()`, `mutate()`,
#' etc. to transform the data before it is written if you need to.
#' @param path string path, URI, or `SubTreeFileSystem` referencing a directory
#' to write to (directory will be created if it does not exist)
#' @param format a string identifier of the file format. Default is to use
#' "parquet" (see [FileFormat])
#' @param partitioning `Partitioning` or a character vector of columns to
#' use as partition keys (to be written as path segments). Default is to
#' use the current `group_by()` columns.
#' @param basename_template string template for the names of files to be written.
#' Must contain `"{i}"`, which will be replaced with an autoincremented
#' integer to generate basenames of datafiles. For example, `"part-{i}.arrow"`
#' will yield `"part-0.arrow", ...`.
#' If not specified, it defaults to `"part-{i}.<default extension>"`.
#' @param hive_style logical: write partition segments as Hive-style
#' (`key1=value1/key2=value2/file.ext`) or as just bare values. Default is `TRUE`.
#' @param existing_data_behavior The behavior to use when there is already data
#' in the destination directory.  Must be one of "overwrite", "error", or
#' "delete_matching".
#' - "overwrite" (the default) then any new files created will overwrite
#'   existing files
#' - "error" then the operation will fail if the destination directory is not
#'   empty
#' - "delete_matching" then the writer will delete any existing partitions
#'   if data is going to be written to those partitions and will leave alone
#'   partitions which data is not written to.
#' @param max_partitions maximum number of partitions any batch may be
#' written into. Default is 1024L.
#' @param max_open_files maximum number of files that can be left opened
#' during a write operation. If greater than 0 then this will limit the
#' maximum number of files that can be left open. If an attempt is made to open
#' too many files then the least recently used file will be closed.
#' If this setting is set too low you may end up fragmenting your data
#' into many small files. The default is 900 which also allows some # of files to be
#' open by the scanner before hitting the default Linux limit of 1024.
#' @param max_rows_per_file maximum number of rows per file.
#' If greater than 0 then this will limit how many rows are placed in any single file.
#' Default is 0L.
#' @param min_rows_per_group write the row groups to the disk when this number of
#' rows have accumulated. Default is 0L.
#' @param max_rows_per_group maximum rows allowed in a single
#' group and when this number of rows is exceeded, it is split and the next set
#' of rows is written to the next group. This value must be set such that it is
#' greater than `min_rows_per_group`. Default is 1024 * 1024.
#' @param ... additional format-specific arguments. For available Parquet
#' options, see [write_parquet()]. The available Feather options are:
#' - `use_legacy_format` logical: write data formatted so that Arrow libraries
#'   versions 0.14 and lower can read it. Default is `FALSE`. You can also
#'   enable this by setting the environment variable `ARROW_PRE_0_15_IPC_FORMAT=1`.
#' - `metadata_version`: A string like "V5" or the equivalent integer indicating
#'   the Arrow IPC MetadataVersion. Default (`NULL`) will use the latest version,
#'   unless the environment variable `ARROW_PRE_1_0_METADATA_VERSION=1`, in
#'   which case it will be V4.
#' - `codec`: A [Codec] which will be used to compress body buffers of written
#'   files. Default (NULL) will not compress body buffers.
#' - `null_fallback`: character to be used in place of missing values (`NA` or
#' `NULL`) when using Hive-style partitioning. See [hive_partition()].
#' @return The input `dataset`, invisibly
#' @examplesIf arrow_with_dataset() & arrow_with_parquet() & requireNamespace("dplyr", quietly = TRUE)
#' # You can write datasets partitioned by the values in a column (here: "cyl").
#' # This creates a structure of the form cyl=X/part-Z.parquet.
#' one_level_tree <- tempfile()
#' write_dataset(mtcars, one_level_tree, partitioning = "cyl")
#' list.files(one_level_tree, recursive = TRUE)
#'
#' # You can also partition by the values in multiple columns
#' # (here: "cyl" and "gear").
#' # This creates a structure of the form cyl=X/gear=Y/part-Z.parquet.
#' two_levels_tree <- tempfile()
#' write_dataset(mtcars, two_levels_tree, partitioning = c("cyl", "gear"))
#' list.files(two_levels_tree, recursive = TRUE)
#'
#' # In the two previous examples we would have:
#' # X = {4,6,8}, the number of cylinders.
#' # Y = {3,4,5}, the number of forward gears.
#' # Z = {0,1,2}, the number of saved parts, starting from 0.
#'
#' # You can obtain the same result as as the previous examples using arrow with
#' # a dplyr pipeline. This will be the same as two_levels_tree above, but the
#' # output directory will be different.
#' library(dplyr)
#' two_levels_tree_2 <- tempfile()
#' mtcars %>%
#'   group_by(cyl, gear) %>%
#'   write_dataset(two_levels_tree_2)
#' list.files(two_levels_tree_2, recursive = TRUE)
#'
#' # And you can also turn off the Hive-style directory naming where the column
#' # name is included with the values by using `hive_style = FALSE`.
#'
#' # Write a structure X/Y/part-Z.parquet.
#' two_levels_tree_no_hive <- tempfile()
#' mtcars %>%
#'   group_by(cyl, gear) %>%
#'   write_dataset(two_levels_tree_no_hive, hive_style = FALSE)
#' list.files(two_levels_tree_no_hive, recursive = TRUE)
#' @export
write_dataset <- function(dataset,
                          path,
                          format = c("parquet", "feather", "arrow", "ipc", "csv"),
                          partitioning = dplyr::group_vars(dataset),
                          basename_template = paste0("part-{i}.", as.character(format)),
                          hive_style = TRUE,
                          existing_data_behavior = c("overwrite", "error", "delete_matching"),
                          max_partitions = 1024L,
                          max_open_files = 900L,
                          max_rows_per_file = 0L,
                          min_rows_per_group = 0L,
                          max_rows_per_group = bitwShiftL(1, 20),
                          ...) {
  format <- match.arg(format)
  if (format %in% c("feather", "ipc")) {
    format <- "arrow"
  }
  if (inherits(dataset, "arrow_dplyr_query")) {
    # partitioning vars need to be in the `select` schema
    dataset <- ensure_group_vars(dataset)
  } else {
    check_named_cols(dataset)
    if (inherits(dataset, "grouped_df")) {
      force(partitioning)
      # Drop the grouping metadata before writing; we've already consumed it
      # now to construct `partitioning` and don't want it in the metadata$r
      dataset <- dplyr::ungroup(dataset)
    }
    dataset <- as_adq(dataset)
  }

  plan <- ExecPlan$create()
  on.exit(plan$.unsafe_delete())

  final_node <- plan$Build(dataset)
  if (!is.null(final_node$extras$sort %||% final_node$extras$head %||% final_node$extras$tail)) {
    # Because sorting and topK are only handled in the SinkNode (or in R!),
    # they wouldn't get picked up in the WriteNode. So let's Run this ExecPlan
    # to capture those, and then create a new plan for writing
    # TODO(ARROW-15681): do sorting in WriteNode in C++
    dataset <- as_adq(plan$Run(final_node))
    plan <- ExecPlan$create()
    final_node <- plan$Build(dataset)
  }

  if (!inherits(partitioning, "Partitioning")) {
    partition_schema <- final_node$schema[partitioning]
    if (isTRUE(hive_style)) {
      partitioning <- HivePartitioning$create(
        partition_schema,
        null_fallback = list(...)$null_fallback
      )
    } else {
      partitioning <- DirectoryPartitioning$create(partition_schema)
    }
  }

  path_and_fs <- get_path_and_filesystem(path)
  output_schema <- final_node$schema
  options <- FileWriteOptions$create(
    format,
    column_names = names(output_schema),
    ...
  )

  # TODO(ARROW-16200): expose FileSystemDatasetWriteOptions in R
  # and encapsulate this logic better
  existing_data_behavior_opts <- c("delete_matching", "overwrite", "error")
  existing_data_behavior <- match(match.arg(existing_data_behavior), existing_data_behavior_opts) - 1L

  if (!missing(max_rows_per_file) && missing(max_rows_per_group) && max_rows_per_group > max_rows_per_file) {
    max_rows_per_group <- max_rows_per_file
  }

  validate_positive_int_value(max_partitions)
  validate_positive_int_value(max_open_files)
  validate_positive_int_value(min_rows_per_group)
  validate_positive_int_value(max_rows_per_group)

  plan$Write(
    final_node,
    options, path_and_fs$fs, path_and_fs$path,
    partitioning, basename_template,
    existing_data_behavior, max_partitions,
    max_open_files, max_rows_per_file,
    min_rows_per_group, max_rows_per_group
  )
}

validate_positive_int_value <- function(value, msg) {
  if (!is_integerish(value, n = 1) || is.na(value) || value < 0) {
    abort(paste(substitute(value), "must be a positive, non-missing integer"))
  }
}
