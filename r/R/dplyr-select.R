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
  check_select_helpers(enexprs(...))
  column_select(as_adq(.data), !!!enquos(...))
}
select.Dataset <- select.ArrowTabular <- select.RecordBatchReader <- select.arrow_dplyr_query

rename.arrow_dplyr_query <- function(.data, ...) {
  check_select_helpers(enexprs(...))
  column_select(as_adq(.data), !!!enquos(...), .FUN = vars_rename)
}
rename.Dataset <- rename.ArrowTabular <- rename.RecordBatchReader <- rename.arrow_dplyr_query

rename_with.arrow_dplyr_query <- function(.data, .fn, .cols = everything(), ...) {
  .fn <- as_function(.fn)
  old_names <- names(dplyr::select(.data, {{ .cols }}))
  dplyr::rename(.data, !!set_names(old_names, .fn(old_names)))
}
rename_with.Dataset <- rename_with.ArrowTabular <- rename_with.RecordBatchReader <- rename_with.arrow_dplyr_query

column_select <- function(.data, ..., .FUN = vars_select) {
  # .FUN is either tidyselect::vars_select or tidyselect::vars_rename
  # It operates on the names() of selected_columns, i.e. the column names
  # factoring in any renaming that may already have happened
  out <- .FUN(names(.data), !!!enquos(...))
  # Make sure that the resulting selected columns map back to the original data,
  # as in when there are multiple renaming steps
  .data$selected_columns <- set_names(.data$selected_columns[out], names(out))

  # If we've renamed columns, we need to project that renaming into other
  # query parameters we've collected
  renamed <- out[names(out) != out]
  if (length(renamed)) {
    # Massage group_by
    gbv <- .data$group_by_vars
    renamed_groups <- gbv %in% renamed
    gbv[renamed_groups] <- names(renamed)[match(gbv[renamed_groups], renamed)]
    .data$group_by_vars <- gbv
    # No need to massage filters because those contain references to Arrow objects
  }
  .data
}

relocate.arrow_dplyr_query <- function(.data, ..., .before = NULL, .after = NULL) {
  # The code in this function is adapted from the code in dplyr::relocate.data.frame
  # at https://github.com/tidyverse/dplyr/blob/master/R/relocate.R
  # TODO: revisit this after https://github.com/tidyverse/dplyr/issues/5829

  .data <- as_adq(.data)

  # Assign the schema to the expressions
  map(.data$selected_columns, ~ (.$schema <- .data$.data$schema))

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

check_select_helpers <- function(exprs) {
  # Throw an error if unsupported tidyselect selection helpers in `exprs`
  exprs <- lapply(exprs, function(x) if (is_quosure(x)) quo_get_expr(x) else x)
  unsup_select_helpers <- "where"
  funs_in_exprs <- unlist(lapply(exprs, all_funs))
  unsup_funs <- funs_in_exprs[funs_in_exprs %in% unsup_select_helpers]
  if (length(unsup_funs)) {
    stop(
      "Unsupported selection ",
      ngettext(length(unsup_funs), "helper: ", "helpers: "),
      oxford_paste(paste0(unsup_funs, "()"), quote = FALSE),
      call. = FALSE
    )
  }
}
