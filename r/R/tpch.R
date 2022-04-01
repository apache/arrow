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

tpch_tables <- c("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")


#' Generate a RecordBatchReader with TPC-H data in it
#'
#' @param table the table to generate
#' @param scale_factor the scale factor to generate
#'
#' @return a RecordBatchReader that will contain the generated data
#' @export
#'
#' @keywords internal
tpch_dbgen <- function(table = tpch_tables, scale_factor) {
  table <- match.arg(table)

  Tpch_Dbgen(ExecPlan$create(), scale_factor, table)
}

#' Generate TPC-H data as datasets
#'
#' This will generate all of the TPC-H data as a set of datasets for a given
#' `scale_factor`
#'
#' @param scale_factor numeric, scale factor of data to generate
#' @param path
#' @param ...
#'
#' @return the path to the directory containing the datasets
#' @export
#' @keywords internal
tpch_dbgen_write <- function(scale_factor, path, ...) {
  path_and_fs <- get_path_and_filesystem(path)
  folder_name <- paste0("/tpch_", format(scale_factor, scientific = FALSE))

  Tpch_Dbgen_Write(
    ExecPlan$create(),
    scale_factor,
    path_and_fs$fs,
    path_and_fs$path,
    folder_name
  )

  invisible(file.path(path_and_fs$path, folder_name))
}
