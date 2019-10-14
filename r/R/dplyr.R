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
  .data$filtered_rows <- c(.data$filtered_rows, quos(...))
  .data
}

#' @importFrom dplyr collect grouped_df
#' @export
collect.RecordBatch <- function(x, ...) {
  filters <- evaluate_filters(x)
  colnames <- names(x)
  for (q in x$selected_columns) {
    colnames <- vars_select(colnames, !!!q)
  }
  df <- as.data.frame(x[filters, colnames])
  if (length(x$group_by_vars)) {
    df <- grouped_df(df, groups(x)$group_names)
  }
  # Slight hack: since x is R6, each select/filter modified the object in place,
  # which is not standard R behavior. Let's zero out x$selected_columns and
  # x$filtered_rows and hope that this side effect cancels out the other
  # unexpected side effects.
  x$selected_columns <- list()
  x$filtered_rows <- list()
  ungroup(x)
  df
}

evaluate_filters <- function(x) {
  if (length(x$filtered_rows) == 0) {
    # Keep everything
    return(TRUE)
  }
  # Cache the columns we need here
  filter_data <- new.env()
  for (v in unique(unlist(lapply(x$filtered_rows, all.vars)))) {
    assign(v, x[[v]], env = filter_data)
  }
  # Evaluate to get Expressions, then pull to R
  filters <- lapply(x$filtered_rows, function (f) {
    expr <- eval_tidy(f, new_data_mask(filter_data))
    # TODO: Call something else that tries to construct a C++ expression
    as.vector(expr)
  })
  Reduce("&", filters)
}

#' @importFrom dplyr summarise summarize
#' @export
summarise.RecordBatch <- function(.data, ...) {
  # TODO: determine whether work can be pushed down to Arrow
  return(summarise(collect(.data), ...))
}

#' @importFrom dplyr group_by group_by_prepare
#' @export
group_by.RecordBatch <- function(.data, ..., add = FALSE) {
  .data$group_by_vars <- group_by_prepare(.data, ..., add = add)
  .data
}

#' @importFrom dplyr groups
#' @export
groups.RecordBatch <- function(x) x$group_by_vars

#' @importFrom dplyr ungroup
#' @export
ungroup.RecordBatch <- function(x, ...) {
  x$group_by_vars <- list()
  x
}
