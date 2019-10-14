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

#' @importFrom dplyr select
#' @export
select.RecordBatch <- function(.data, ...) {
  .data$selected_columns <- c(.data$selected_columns, list(quos(...)))
  .data
}

#' @importFrom dplyr filter
#' @export
filter.RecordBatch <- function(.data, ..., .preserve = FALSE) {
  .data$filtered_rows <- c(.data$filtered_rows, list(quos(...)))
  .data
}

#' @importFrom dplyr collect
#' @export
collect.RecordBatch <- function(x, ...) {
  colnames <- names(x)
  for (q in x$selected_columns) {
    colnames <- vars_select(colnames, !!!q)
  }
  # TODO: evaluate filters before as.data.frame
  df <- as.data.frame(x[, colnames])
  for (f in x$filtered_rows) {
    df <- filter(df, !!!f)
  }
  # Slight hack: since x is R6, each select/filter modified the object in place,
  # which is not standard R behavior. Let's zero out x$selected_columns and
  # x$filtered_rows and hope that this side effect cancels out the other
  # unexpected side effects.
  x$selected_columns <- list()
  x$filtered_rows <- list()
  df
}

#' @importFrom dplyr summarise summarize
#' @export
summarise.RecordBatch <- function (.data, ...) {
    # TODO: determine whether work can be pushed down to Arrow
    return(summarise(collect(.data), ...))
}
