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

#' Read parquet file from disk
#'
#' @param file a file path
#' @param as_tibble should the [arrow::Table][arrow__Table] be converted to a tibble.
#' @param use_threads Use threads when converting to a tibble, only relevant if `as_tibble` is `TRUE`
#' @param ... currently ignored
#'
#' @return a [arrow::Table][arrow__Table], or a data frame if `as_tibble` is `TRUE`.
#'
#' @export
read_parquet <- function(file, as_tibble = TRUE, use_threads = TRUE, ...) {
  tab <- shared_ptr(`arrow::Table`, read_parquet_file(file))
  if (isTRUE(as_tibble)) {
    tab <- as_tibble(tab, use_threads = use_threads)
  }
  tab
}
