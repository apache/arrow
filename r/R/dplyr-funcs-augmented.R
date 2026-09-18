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

#' Add the data filename as a column
#'
#' This function only exists inside `arrow` `dplyr` queries, and it only is
#' valid when querying on a `FileSystemDataset`, such as one created by
#' [open_dataset()]. Use it inside `mutate()` to add a column holding the path
#' of the file each row was read from.
#'
#' The filename column can be used in later `mutate()`, `select()`, `arrange()`
#' and `group_by()` steps of the same query, but it cannot be used in a
#' `filter()` until you have called \code{\link[dplyr:compute]{compute()}} or
#' \code{\link[dplyr:collect]{collect()}}. It must also be added before any
#' aggregation or join. See Examples.
#'
#' @return A `FieldRef` \code{\link{Expression}} that refers to the filename
#' augmented column.
#'
#' @seealso [open_dataset()]
#'
#' @examples \dontrun{
#' open_dataset("nyc-taxi") |>
#'   mutate(file = add_filename()) |>
#'   collect()
#'
#' # The new column can be used in a later mutate() in the same query
#' open_dataset("nyc-taxi") |>
#'   mutate(file = add_filename()) |>
#'   mutate(filename_length = nchar(file)) |>
#'   collect()
#'
#' # To filter() on the new column, call compute() or collect() first
#' open_dataset("nyc-taxi") |>
#'   mutate(file = add_filename()) |>
#'   compute() |>
#'   filter(endsWith(file, "part-0.parquet")) |>
#'   collect()
#' }
#'
#' @keywords internal
add_filename <- function() Expression$field_ref("__filename")

register_bindings_augmented <- function() {
  register_binding("arrow::add_filename", add_filename)
}
