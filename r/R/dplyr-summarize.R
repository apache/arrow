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


# The following S3 methods are registered on load if dplyr is present

summarise.arrow_dplyr_query <- function(.data, ..., .engine = c("arrow", "duckdb")) {
  call <- match.call()
  .data <- arrow_dplyr_query(.data)
  exprs <- quos(...)
  # Only retain the columns we need to do our aggregations
  vars_to_keep <- unique(c(
    unlist(lapply(exprs, all.vars)),     # vars referenced in summarise
    dplyr::group_vars(.data)             # vars needed for grouping
  ))
  .data <- dplyr::select(.data, vars_to_keep)

  if (match.arg(.engine) == "duckdb") {
    dplyr::summarise(to_duckdb(.data), ...)
  } else {
    if (query_on_dataset(.data)) {
      not_implemented_for_dataset("summarize()")
    }
    dplyr::summarise(dplyr::collect(.data), ...)
  }
}
summarise.Dataset <- summarise.ArrowTabular <- summarise.arrow_dplyr_query
