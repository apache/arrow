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
  out <- compute.arrow_dplyr_query(x)
  collect.ArrowTabular(out, as_data_frame)
}
collect.ArrowTabular <- function(x, as_data_frame = TRUE, ...) {
  if (as_data_frame) {
    df <- x$to_data_frame()
    apply_arrow_r_metadata(df, x$metadata$r)
  } else {
    x
  }
}
collect.Dataset <- function(x, as_data_frame = TRUE, ...) {
  collect.ArrowTabular(compute.Dataset(x), as_data_frame)
}
collect.RecordBatchReader <- collect.Dataset

collect.StructArray <- function(x, row.names = NULL, optional = FALSE, ...) {
  as.vector(x)
}

compute.ArrowTabular <- function(x, ...) x
compute.arrow_dplyr_query <- function(x, ...) {
  # TODO: should this tryCatch move down into as_arrow_table()?
  tryCatch(
    as_arrow_table(x),
    # n = 4 because we want the error to show up as being from compute()
    # and not augment_io_error_msg()
    error = function(e, call = caller_env(n = 4)) {
      # Use a dummy schema() here because the CSV file reader handler is only
      # valid when you read_csv_arrow() with a schema, but Dataset always has
      # schema
      # TODO: clean up this
      augment_io_error_msg(e, call, schema = schema())
    }
  )
}
compute.Dataset <- compute.RecordBatchReader <- compute.arrow_dplyr_query

pull.Dataset <- function(.data,
                         var = -1,
                         ...,
                         as_vector = getOption("arrow.pull_as_vector")) {
  .data <- as_adq(.data)
  var <- vars_pull(names(.data), !!enquo(var))
  .data$selected_columns <- set_names(.data$selected_columns[var], var)
  out <- dplyr::compute(.data)[[1]]
  handle_pull_as_vector(out, as_vector)
}
pull.RecordBatchReader <- pull.arrow_dplyr_query <- pull.Dataset

pull.ArrowTabular <- function(x,
                              var = -1,
                              ...,
                              as_vector = getOption("arrow.pull_as_vector")) {
  out <- x[[vars_pull(names(x), !!enquo(var))]]
  handle_pull_as_vector(out, as_vector)
}

handle_pull_as_vector <- function(out, as_vector) {
  if (is.null(as_vector)) {
    warn(
      c(
        paste(
          "Default behavior of `pull()` on Arrow data is changing. Current",
          "behavior of returning an R vector is deprecated, and in a future",
          "release, it will return an Arrow `ChunkedArray`. To control this:"
        ),
        i = paste(
          "Specify `as_vector = TRUE` (the current default) or",
          "`FALSE` (what it will change to) in `pull()`"
        ),
        i = "Or, set `options(arrow.pull_as_vector)` globally"
      ),
      .frequency = "regularly",
      .frequency_id = "arrow.pull_as_vector",
      class = "lifecycle_warning_deprecated"
    )
    as_vector <- TRUE
  }
  if (as_vector) {
    out <- as.vector(out)
  }
  out
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

  if (is.null(.data$aggregations) && is.null(.data$join) && !needs_projection(.data$selected_columns, old_schm)) {
    # Just use the schema we have
    return(old_schm)
  }

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

      # If keep = TRUE, we want to keep the key columns in the RHS. Otherwise,
      # they will be dropped. Also, if the join is a full join, then we are
      # temporarily keeping the key columns so we can coalesce them after.
      if (.data$join$keep || .data$join$type == JoinType$FULL_OUTER) {
        # find the common column names in left and right tables
        common_cols <- intersect(names(right_cols), names(left_cols))
        right_fields <- map(right_cols, ~ .$type(.data$join$right_data$.data$schema))
      } else {
        right_fields <- map(
          right_cols[setdiff(names(right_cols), .data$join$by)],
          ~ .$type(.data$join$right_data$.data$schema)
        )
        # get right table and left table column projections excluding the join key(s)
        right_cols_ex_by <- right_cols[setdiff(names(right_cols), .data$join$by)]
        left_cols_ex_by <- left_cols[setdiff(names(left_cols), .data$join$by)]
        # find the common column names in left and right tables
        common_cols <- intersect(names(right_cols_ex_by), names(left_cols_ex_by))
      }

      # adding suffixes to the common columns in left and right tables
      left_fields <- add_suffix(new_fields, common_cols, .data$join$suffix[[1]])
      right_fields <- add_suffix(right_fields, common_cols, .data$join$suffix[[2]])
      new_fields <- c(left_fields, right_fields)
    }
  } else {
    hash <- length(.data$group_by_vars) > 0
    # The output schema is based on the aggregations and any group_by vars.
    # The group_by vars come first.
    new_fields <- c(
      group_types(.data, old_schm),
      aggregate_types(.data, hash, old_schm)
    )
  }
  schema(!!!new_fields)
}
