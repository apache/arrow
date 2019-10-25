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

select.RecordBatch <- function(.data, ...) {
  # This S3 method is registered on load if dplyr is present
  .data <- .data$clone()
  .data$selected_columns <- c(.data$selected_columns, list(quos(...)))
  .data
}
select.Table <- select.RecordBatch

#' @importFrom tidyselect vars_rename
rename.RecordBatch <- function(.data, ...) {
  # This S3 method is registered on load if dplyr is present
  dplyr::select(.data, vars_rename(names(.data), !!!enquos(...)))
}
rename.Table <- rename.RecordBatch

filter.RecordBatch <- function(.data, ..., .preserve = FALSE) {
  # This S3 method is registered on load if dplyr is present
  .data <- .data$clone()
  .data$filtered_rows <- c(.data$filtered_rows, quos(...))
  .data
}
filter.Table <- filter.RecordBatch

collect.RecordBatch <- function(x, ...) {
  # This S3 method is registered on load if dplyr is present

  # First, evaluate any filters and turn into a logical vector
  filters <- evaluate_filters(x)

  # Second, figure out what columns are needed, including possible renamings
  colnames <- evaluate_select(x)
  # Be sure to retain any group_by vars
  gv <- setdiff(dplyr::group_vars(x), colnames)
  if (length(gv)) {
    colnames <- c(colnames, stats::setNames(gv, gv))
  }

  # Then, pull only the selected rows and cols into R
  df <- as.data.frame(x[filters, colnames])
  # In case variables were renamed, apply those names
  names(df) <- names(colnames)

  # Preserve groupings, if present
  if (length(x$group_by_vars)) {
    df <- dplyr::grouped_df(df, dplyr::groups(x))
  }
  df
}
collect.Table <- collect.RecordBatch

#' @importFrom tidyselect vars_pull
pull.RecordBatch <- function(.data, var = -1) {
  # This S3 method is registered on load if dplyr is present
  dplyr::collect(dplyr::select(.data, vars_pull(evaluate_select(.data), !!enquo(var))))[[1]]
}
pull.Table <- pull.RecordBatch

evaluate_select <- function(x) {
  colnames <- stats::setNames(names(x), names(x))
  for (q in x$selected_columns) {
    # If columns are renamed, the new names appear in names(colnames)
    colnames <- vars_select(colnames, !!!q)
  }
  colnames
}

evaluate_filters <- function(x) {
  if (length(x$filtered_rows) == 0) {
    # Keep everything
    return(TRUE)
  }
  # Grab the Arrow Arrays we need in order to evaluate the filter expressions
  filter_data <- env()
  for (v in unique(unlist(lapply(x$filtered_rows, all.vars)))) {
    this <- x[[v]]
    if (is.null(this)) {
      stop("object '", v, "' not found", call. = FALSE)
    }
    # TODO: when we can evaluate these expressions in the C++ lib,
    # don't as.vector here: just grab the array so that eval_tidy below
    # yields an Expression
    assign(v, as.vector(this), envir = filter_data)
  }
  dm <- new_data_mask(filter_data)
  filters <- lapply(x$filtered_rows, function (f) {
    eval_tidy(f, dm)
    # TODO: when that's an Expression, call as.vector on it here to evaluate
  })
  # filters is a list of logical vectors corresponding to each of the exprs.
  # AND them together and return
  Reduce("&", filters)
}

summarise.RecordBatch <- function(.data, ...) {
  # This S3 method is registered on load if dplyr is present
  # Only retain the columns we need to do our aggregations
  vars_to_keep <- unique(c(
    unlist(lapply(quos(...), all.vars)), # vars referenced in summarise
    dplyr::group_vars(.data)             # vars needed for grouping
  ))
  .data <- dplyr::select(.data, vars_to_keep)
  # TODO: determine whether work can be pushed down to Arrow
  dplyr::summarise(dplyr::collect(.data), ...)
}
summarise.Table <- summarise.RecordBatch

group_by.RecordBatch <- function(.data, ..., add = FALSE) {
  # This S3 method is registered on load if dplyr is present
  .data <- .data$clone()
  .data$group_by_vars <- dplyr::group_by_prepare(.data, ..., add = add)$group_names
  .data
}
group_by.Table <- group_by.RecordBatch

# This S3 method is registered on load if dplyr is present
groups.RecordBatch <- function(x) syms(dplyr::group_vars(x))
groups.Table <- groups.RecordBatch

# This S3 method is registered on load if dplyr is present
group_vars.RecordBatch <- function(x) x$group_by_vars
group_vars.Table <- group_vars.RecordBatch

ungroup.RecordBatch <- function(x, ...) {
  # This S3 method is registered on load if dplyr is present
  x$group_by_vars <- character()
  x
}
ungroup.Table <- ungroup.RecordBatch

mutate.RecordBatch <- function(.data, ...) {
  # This S3 method is registered on load if dplyr is present
  dplyr::mutate(dplyr::collect(.data), ...)
}
mutate.Table <- mutate.RecordBatch

arrange.RecordBatch <- function(.data, ...) {
  # This S3 method is registered on load if dplyr is present
  dplyr::arrange(dplyr::collect(.data), ...)
}
arrange.Table <- arrange.RecordBatch
