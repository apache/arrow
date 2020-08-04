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
#' @param dataset [Dataset] or `arrow_dplyr_query`. If a `arrow_dplyr_query`,
#' note that `select()` or `filter()` queries are not currently supported.
#' @param path string path to a directory to write to (directory will be
#' created if it does not exist)
#' @param format file format to write the dataset to. Currently only "feather"
#' (aka "ipc") is supported.
#' @param partitioning `Partitioning` or a character vector of columns to
#' use as partition keys (to be written as path segments). Default is to
#' use the current `group_by()` columns.
#' @param hive_style logical: write partition segments as Hive-style
#' (`key1=value1/key2=value2/file.ext`) or as just bare values. Default is `TRUE`.
#' @param ... additional arguments, passed to `dataset$write()`
#' @return The input `dataset`, invisibly
#' @export
write_dataset <- function(dataset,
                          path,
                          format = dataset$format$type,
                          partitioning = dplyr::group_vars(dataset),
                          hive_style = TRUE,
                          ...) {
  if (inherits(dataset, "arrow_dplyr_query")) {
    force(partitioning) # get the group_vars before we drop the object
    # TODO: Write a filtered/projected dataset
    if (!isTRUE(dataset$filtered_rows)) {
      stop("Writing a filtered dataset is not yet supported", call. = FALSE)
    }
    if (!identical(dataset$selected_columns, set_names(names(dataset$.data)))) {
      # TODO: actually, we can do this?
      stop("TODO", call. = FALSE)
    }
    dataset <- dataset$.data
  }
  if (!inherits(dataset, "Dataset")) {
    stop("'dataset' must be a Dataset", call. = FALSE)
    # TODO: This does not exist yet (in the R bindings at least)
    # dataset <- InMemoryDataset$create(dataset)
  }

  if (!inherits(format, "FileFormat")) {
    format <- FileFormat$create(format, ...)
  }
  if (!inherits(format, "IpcFileFormat")) {
    stop(
      "Unsupported format; datasets currently can only be written to IPC/Feather format",
      call. = FALSE
    )
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
  dataset$write(path, format = format, partitioning = partitioning, ...)
}
