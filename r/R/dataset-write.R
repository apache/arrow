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
#' and `group_by()` operations done on the dataset. Note that `filter()` queries
#' are not currently supported, and `select`-ed columns may not be renamed.
#' @param path string path to a directory to write to (directory will be
#' created if it does not exist)
#' @param format file format to write the dataset to. Currently supported
#' formats are "feather" (aka "ipc") and "parquet". Default is to write to the
#' same format as `dataset`.
#' @param schema [Schema] containing a subset of columns, possibly reordered,
#' in `dataset`. Default is `dataset$schema`, i.e. all columns.
#' @param partitioning `Partitioning` or a character vector of columns to
#' use as partition keys (to be written as path segments). Default is to
#' use the current `group_by()` columns.
#' @param hive_style logical: write partition segments as Hive-style
#' (`key1=value1/key2=value2/file.ext`) or as just bare values. Default is `TRUE`.
#' @param ... additional format-specific arguments. For available Parquet
#' options, see [write_parquet()].
#' @return The input `dataset`, invisibly
#' @export
write_dataset <- function(dataset,
                          path,
                          format = dataset$format$type,
                          schema = dataset$schema,
                          partitioning = dplyr::group_vars(dataset),
                          hive_style = TRUE,
                          ...) {
  if (inherits(dataset, "arrow_dplyr_query")) {
    force(partitioning) # get the group_vars before we drop the object
    # Check for a filter
    if (!isTRUE(dataset$filtered_rows)) {
      # TODO:
      stop("Writing a filtered dataset is not yet supported", call. = FALSE)
    }
    # Check for a select
    if (!identical(dataset$selected_columns, set_names(names(dataset$.data)))) {
      # We can select a subset of columns but we can't rename them
      if (!setequal(dataset$selected_columns, names(dataset$selected_columns))) {
        stop("Renaming columns when writing a dataset is not yet supported", call. = FALSE)
      }
      dataset <- ensure_group_vars(dataset)
      schema <- dataset$.data$schema[dataset$selected_columns]
    }
    dataset <- dataset$.data
  }
  if (inherits(dataset, c("data.frame", "RecordBatch", "Table"))) {
    force(partitioning) # get the group_vars before we replace the object
    if (inherits(dataset, "grouped_df")) {
      # Drop the grouping metadata before writing; we've already consumed it
      # now to construct `partitioning` and don't want it in the metadata$r
      dataset <- dplyr::ungroup(dataset)
    }
    dataset <- InMemoryDataset$create(dataset)
  }
  if (!inherits(dataset, "Dataset")) {
    stop("'dataset' must be a Dataset, not ", class(dataset)[1], call. = FALSE)
  }

  if (!inherits(format, "FileFormat")) {
    if (identical(format, "parquet")) {
      # We have to do some special massaging of properties
      writer_props <- ParquetWriterProperties$create(dataset, ...)
      arrow_writer_props <- ParquetArrowWriterProperties$create(...)
      format <- ParquetFileFormat$create(writer_properties = writer_props, arrow_writer_properties = arrow_writer_props)
    } else {
      format <- FileFormat$create(format, ...)
    }
  }

  if (!inherits(partitioning, "Partitioning")) {
    # TODO: tidyselect?
    partition_schema <- dataset$schema[partitioning]
    if (isTRUE(hive_style)) {
      partitioning <- HivePartitioning$create(partition_schema)
    } else {
      partitioning <- DirectoryPartitioning$create(partition_schema)
    }
  }
  dataset$write(path, format = format, partitioning = partitioning, schema = schema, ...)
}
