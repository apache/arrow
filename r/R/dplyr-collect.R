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

collect.arrow_dplyr_query <- function(x, as_data_frame = TRUE, ...) {
  tryCatch(
    out <- as_arrow_table(x),
    # n = 4 because we want the error to show up as being from collect()
    # and not augment_io_error_msg()
    error = function(e, call = caller_env(n = 4)) {
      augment_io_error_msg(e, call, schema = x$.data$schema)
    }
  )

  if (as_data_frame) {
    out <- as.data.frame(out)
  }
  restore_dplyr_features(out, x)
}
collect.ArrowTabular <- function(x, as_data_frame = TRUE, ...) {
  if (as_data_frame) {
    as.data.frame(x, ...)
  } else {
    x
  }
}
collect.Dataset <- collect.RecordBatchReader <- function(x, ...) dplyr::collect(as_adq(x), ...)

compute.arrow_dplyr_query <- function(x, ...) dplyr::collect(x, as_data_frame = FALSE)
compute.ArrowTabular <- function(x, ...) x
compute.Dataset <- compute.RecordBatchReader <- compute.arrow_dplyr_query

pull.arrow_dplyr_query <- function(.data, var = -1) {
  .data <- as_adq(.data)
  var <- vars_pull(names(.data), !!enquo(var))
  .data$selected_columns <- set_names(.data$selected_columns[var], var)
  dplyr::collect(.data)[[1]]
}
pull.Dataset <- pull.ArrowTabular <- pull.RecordBatchReader <- pull.arrow_dplyr_query

restore_dplyr_features <- function(df, query) {
  # An arrow_dplyr_query holds some attributes that Arrow doesn't know about
  # After calling collect(), make sure these features are carried over

  if (length(query$group_by_vars) > 0) {
    # Preserve groupings, if present
    if (is.data.frame(df)) {
      df <- dplyr::grouped_df(
        df,
        dplyr::group_vars(query),
        drop = dplyr::group_by_drop_default(query)
      )
    } else {
      # This is a Table, via compute() or collect(as_data_frame = FALSE)
      df$metadata$r$attributes$.group_vars <- query$group_by_vars
    }
  }
  df
}

collapse.arrow_dplyr_query <- function(x, ...) {
  # Figure out what schema will result from the query
  x$schema <- implicit_schema(x)
  # Nest inside a new arrow_dplyr_query (and keep groups)
  out <- arrow_dplyr_query(x)
  out$group_by_vars <- x$group_by_vars
  out$drop_empty_groups <- x$drop_empty_groups
  out
}
collapse.Dataset <- collapse.ArrowTabular <- collapse.RecordBatchReader <- function(x, ...) {
  arrow_dplyr_query(x)
}

# helper method to add suffix
add_suffix <- function(fields, common_cols, suffix) {
  # helper function which adds the suffixes to the
  # selected column names
  # for join relation the selected columns are the
  # columns with same name in left and right relation
  col_names <- names(fields)
  new_col_names <- map(col_names, function(x) {
    if (is.element(x, common_cols)) {
      paste0(x, suffix)
    } else {
      x
    }
  })
  set_names(fields, new_col_names)
}

implicit_schema <- function(.data) {
  # Get the source data schema so that we can evaluate expressions to determine
  # the output schema. Note that we don't use source_data() because we only
  # want to go one level up (where we may have called implicit_schema() before)
  .data <- ensure_group_vars(.data)
  old_schm <- .data$.data$schema
  # Add in any augmented fields that may exist in the query but not in the
  # real data, in case we have FieldRefs to them
  old_schm[["__filename"]] <- string()

  if (is.null(.data$aggregations)) {
    # .data$selected_columns is a named list of Expressions (FieldRefs or
    # something more complex). Bind them in order to determine their output type
    new_fields <- map(.data$selected_columns, ~ .$type(old_schm))
    if (!is.null(.data$join) && !(.data$join$type %in% JoinType[1:4])) {
      # Add cols from right side, except for semi/anti joins
      right_cols <- .data$join$right_data$selected_columns
      left_cols <- .data$selected_columns
      right_fields <- map(
        right_cols[setdiff(names(right_cols), .data$join$by)],
        ~ .$type(.data$join$right_data$.data$schema)
      )
      # get right table and left table column names excluding the join key
      right_cols_ex_by <- right_cols[setdiff(names(right_cols), .data$join$by)]
      left_cols_ex_by <- left_cols[setdiff(names(left_cols), .data$join$by)]
      # find the common column names in left and right tables
      common_cols <- intersect(names(right_cols_ex_by), names(left_cols_ex_by))
      # adding suffixes to the common columns in left and right tables
      left_fields <- add_suffix(new_fields, common_cols, .data$join$suffix[[1]])
      right_fields <- add_suffix(right_fields, common_cols, .data$join$suffix[[2]])
      new_fields <- c(left_fields, right_fields)
    }
  } else {
    # The output schema is based on the aggregations and any group_by vars
    new_fields <- map(summarize_projection(.data), ~ .$type(old_schm))
    # * Put group_by_vars first (this can't be done by summarize,
    #   they have to be last per the aggregate node signature,
    #   and they get projected to this order after aggregation)
    # * Infer the output types from the aggregations
    group_fields <- new_fields[.data$group_by_vars]
    hash <- length(.data$group_by_vars) > 0
    agg_fields <- imap(
      new_fields[setdiff(names(new_fields), .data$group_by_vars)],
      ~ agg_fun_output_type(.data$aggregations[[.y]][["fun"]], .x, hash)
    )
    new_fields <- c(group_fields, agg_fields)
  }
  schema(!!!new_fields)
}
