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

do_join <- function(x,
                    y,
                    by = NULL,
                    copy = FALSE,
                    suffix = c(".x", ".y"),
                    ...,
                    keep = FALSE,
                    na_matches,
                    join_type) {
  # TODO: handle `copy` arg: ignore?
  # TODO: handle `na_matches` arg
  x <- as_adq(x)
  y <- as_adq(y)
  by <- handle_join_by(by, x, y)

  # For outer joins, we need to output the join keys on both sides so we
  # can coalesce them afterwards.
  left_output <- if (!keep && join_type == "RIGHT_OUTER") {
    setdiff(names(x), by)
  } else {
    names(x)
  }

  right_output <- if (keep || join_type %in% c("FULL_OUTER", "RIGHT_OUTER")) {
    names(y)
  } else {
    setdiff(names(y), by)
  }

  x$join <- list(
    type = JoinType[[join_type]],
    right_data = y,
    by = by,
    left_output = left_output,
    right_output = right_output,
    suffix = suffix,
    keep = keep
  )
  collapse.arrow_dplyr_query(x)
}

left_join.arrow_dplyr_query <- function(x,
                                        y,
                                        by = NULL,
                                        copy = FALSE,
                                        suffix = c(".x", ".y"),
                                        ...,
                                        keep = FALSE) {
  do_join(x, y, by, copy, suffix, ..., keep = keep, join_type = "LEFT_OUTER")
}
left_join.Dataset <- left_join.ArrowTabular <- left_join.RecordBatchReader <- left_join.arrow_dplyr_query

right_join.arrow_dplyr_query <- function(x,
                                         y,
                                         by = NULL,
                                         copy = FALSE,
                                         suffix = c(".x", ".y"),
                                         ...,
                                         keep = FALSE) {
  do_join(x, y, by, copy, suffix, ..., keep = keep, join_type = "RIGHT_OUTER")
}
right_join.Dataset <- right_join.ArrowTabular <- right_join.RecordBatchReader <- right_join.arrow_dplyr_query

inner_join.arrow_dplyr_query <- function(x,
                                         y,
                                         by = NULL,
                                         copy = FALSE,
                                         suffix = c(".x", ".y"),
                                         ...,
                                         keep = FALSE) {
  do_join(x, y, by, copy, suffix, ..., keep = keep, join_type = "INNER")
}
inner_join.Dataset <- inner_join.ArrowTabular <- inner_join.RecordBatchReader <- inner_join.arrow_dplyr_query

full_join.arrow_dplyr_query <- function(x,
                                        y,
                                        by = NULL,
                                        copy = FALSE,
                                        suffix = c(".x", ".y"),
                                        ...,
                                        keep = FALSE) {
  query <- do_join(x, y, by, copy, suffix, ..., keep = keep, join_type = "FULL_OUTER")

  # If we are doing a full outer join and not keeping the join keys of
  # both sides, we need to coalesce. Otherwise, rows that exist in the
  # RHS will have NAs for the join keys.
  if (!keep) {
    query$selected_columns <- post_join_projection(names(x), names(y), handle_join_by(by, x, y), suffix)
  }

  query
}
full_join.Dataset <- full_join.ArrowTabular <- full_join.RecordBatchReader <- full_join.arrow_dplyr_query

semi_join.arrow_dplyr_query <- function(x,
                                        y,
                                        by = NULL,
                                        copy = FALSE,
                                        ...) {
  do_join(x, y, by, copy, suffix = c(".x", ".y"), ..., join_type = "LEFT_SEMI")
}
semi_join.Dataset <- semi_join.ArrowTabular <- semi_join.RecordBatchReader <- semi_join.arrow_dplyr_query

anti_join.arrow_dplyr_query <- function(x,
                                        y,
                                        by = NULL,
                                        copy = FALSE,
                                        ...) {
  do_join(x, y, by, copy, suffix = c(".x", ".y"), ..., join_type = "LEFT_ANTI")
}
anti_join.Dataset <- anti_join.ArrowTabular <- anti_join.RecordBatchReader <- anti_join.arrow_dplyr_query

handle_join_by <- function(by, x, y) {
  if (is.null(by)) {
    return(set_names(intersect(names(x), names(y))))
  }
  if (inherits(by, "dplyr_join_by")) {
    if (!all(by$condition == "==" & by$filter == "none")) {
      abort(
        paste0(
          "Inequality conditions and helper functions ",
          "are not supported in `join_by()` expressions."
        )
      )
    }
    by <- set_names(by$y, by$x)
  }
  stopifnot(is.character(by))
  if (is.null(names(by))) {
    by <- set_names(by)
  }

  missing_x_cols <- setdiff(names(by), names(x))
  missing_y_cols <- setdiff(by, names(y))
  message_x <- NULL
  message_y <- NULL

  if (length(missing_x_cols) > 0) {
    message_x <- paste(
      oxford_paste(missing_x_cols, quote_symbol = "`"),
      "not present in x."
    )
  }

  if (length(missing_y_cols) > 0) {
    message_y <- paste(
      oxford_paste(missing_y_cols, quote_symbol = "`"),
      "not present in y."
    )
  }

  if (length(missing_x_cols) > 0 || length(missing_y_cols) > 0) {
    err_header <- "Join columns must be present in data."
    abort(c(err_header, x = message_x, x = message_y))
  }

  by
}


#' Create projection needed to coalesce join keys after a full outer join
#'
#' @examples
#' test_join <- list(
#'   type = JoinType$FULL_OUTER,
#'   right_data = arrow_table(x = 1, y = 2, z = "x"),
#'   by = c("x", "y"),
#'   suffix = c(".x", ".y"),
#'   keep = FALSE
#' )
#' post_join_projection(c("value", "x", "y", "z"), test_join)
#'
#' @noRd
post_join_projection <- function(left_names, right_names, by, suffix) {
  # Collect mapping of which columns on left need to be coalesced with which
  # column on the right side.
  coalesce_targets <- data.frame(
    left_index = match(by, left_names),
    right_index = match(by, right_names)
  )
  # Right names as output by the join (with suffix if name collided with LHS)
  right_names_input <- ifelse(
    right_names %in% left_names,
    paste0(right_names, suffix[[2]]),
    right_names
  )

  left_exprs <- vector("list", length(left_names))
  for (i in seq_along(left_names)) {
    name <- left_names[i]
    # Name as outputted by the join (with suffix if name collided with RHS)
    name_input <- if (name %in% right_names) {
      paste0(name, suffix[[1]])
    } else {
      name
    }

    if (i %in% coalesce_targets$left_index) {
      target_i <- match(i, coalesce_targets$left_index)
      left_exprs[[i]] <- Expression$create(
        "coalesce",
        Expression$field_ref(name_input),
        Expression$field_ref(right_names_input[coalesce_targets[target_i, 2]])
      )
      # We can drop the suffix that was added
      names(left_exprs)[i] <- name
    } else {
      left_exprs[[i]] <- Expression$field_ref(name_input)
      names(left_exprs)[i] <- name_input
    }
  }

  # Exclude join keys from right side now
  right_names_input <- right_names_input[!(right_names %in% by)]
  right_exprs <- lapply(right_names_input, Expression$field_ref)
  names(right_exprs) <- right_names_input

  c(left_exprs, right_exprs)
}
