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

tbl_vars.arrow_dplyr_query <- function(x) names(x$selected_columns)

select.arrow_dplyr_query <- function(.data, ...) {
  column_select(.data, enquos(...), op = "select")
}
select.Dataset <- select.ArrowTabular <- select.RecordBatchReader <- select.arrow_dplyr_query

rename.arrow_dplyr_query <- function(.data, ...) {
  column_select(.data, enquos(...), op = "rename")
}
rename.Dataset <- rename.ArrowTabular <- rename.RecordBatchReader <- rename.arrow_dplyr_query

rename_with.arrow_dplyr_query <- function(.data, .fn, .cols = everything(), ...) {
  .fn <- as_function(.fn)
  old_names <- names(dplyr::select(.data, {{ .cols }}))
  dplyr::rename(.data, !!set_names(old_names, .fn(old_names)))
}
rename_with.Dataset <- rename_with.ArrowTabular <- rename_with.RecordBatchReader <- rename_with.arrow_dplyr_query

relocate.arrow_dplyr_query <- function(.data, ..., .before = NULL, .after = NULL) {
  # The code in this function is adapted from the code in dplyr::relocate.data.frame
  # at https://github.com/tidyverse/dplyr/blob/main/R/relocate.R
  # TODO: revisit this after https://github.com/tidyverse/dplyr/issues/5829

  .data <- as_adq(.data)

  # Assign the schema to the expressions
  schema <- .data$.data$schema
  walk(.data$selected_columns, ~ (.$schema <- schema))

  # Create a mask for evaluating expressions in tidyselect helpers
  mask <- new_environment(.cache$functions, parent = caller_env())

  to_move <- eval_select(substitute(c(...)), .data$selected_columns, mask)

  .before <- enquo(.before)
  .after <- enquo(.after)
  has_before <- !quo_is_null(.before)
  has_after <- !quo_is_null(.after)

  if (has_before && has_after) {
    abort("Must supply only one of `.before` and `.after`.")
  } else if (has_before) {
    where <- min(unname(eval_select(quo_get_expr(.before), .data$selected_columns, mask)))
    if (!where %in% to_move) {
      to_move <- c(to_move, where)
    }
  } else if (has_after) {
    where <- max(unname(eval_select(quo_get_expr(.after), .data$selected_columns, mask)))
    if (!where %in% to_move) {
      to_move <- c(where, to_move)
    }
  } else {
    where <- 1L
    if (!where %in% to_move) {
      to_move <- c(to_move, where)
    }
  }

  lhs <- setdiff(seq2(1, where - 1), to_move)
  rhs <- setdiff(seq2(where + 1, length(.data$selected_columns)), to_move)

  pos <- vec_unique(c(lhs, to_move, rhs))
  new_names <- names(pos)
  .data$selected_columns <- .data$selected_columns[pos]

  if (!is.null(new_names)) {
    names(.data$selected_columns)[new_names != ""] <- new_names[new_names != ""]
  }
  .data
}
relocate.Dataset <- relocate.ArrowTabular <- relocate.RecordBatchReader <- relocate.arrow_dplyr_query

column_select <- function(.data, select_expression, op = c("select", "rename")) {
  op <- match.arg(op)

  .data <- as_adq(.data)
  sim_df <- as.data.frame(implicit_schema(.data))
  old_names <- names(sim_df)

  if (op == "select") {
    out <- eval_select(expr(c(!!!select_expression)), sim_df)
    # select only columns from `out`
    subset <- out
  } else if (op == "rename") {
    out <- eval_rename(expr(c(!!!select_expression)), sim_df)
    # select all columns as only renaming
    subset <- set_names(seq_along(old_names), old_names)
    names(subset)[out] <- names(out)
  }

  .data$selected_columns <- set_names(.data$selected_columns[subset], names(subset))

  # check if names have updated
  new_names <- old_names
  new_names[out] <- names(out)
  names_compared <- set_names(old_names, new_names)
  renamed <- names_compared[old_names != new_names]

  # Update names in group_by if changed in select() or rename()
  if (length(renamed)) {
    gbv <- .data$group_by_vars
    renamed_groups <- gbv %in% renamed
    gbv[renamed_groups] <- names(renamed)[match(gbv[renamed_groups], renamed)]
    .data$group_by_vars <- gbv
  }

  .data
}
