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

#' @include expression.R
#' @include record-batch.R
#' @include table.R

arrow_dplyr_query <- function(.data) {
  # An arrow_dplyr_query is a container for an Arrow data object (Table,
  # RecordBatch, or Dataset) and the state of the user's dplyr query--things
  # like selected columns, filters, and group vars.

  # For most dplyr methods,
  # method.Table == method.RecordBatch == method.Dataset == method.arrow_dplyr_query
  # This works because the functions all pass .data through arrow_dplyr_query()
  if (inherits(.data, "arrow_dplyr_query")) {
    return(.data)
  }

  # Evaluating expressions on a dataset with duplicated fieldnames will error
  dupes <- duplicated(names(.data))
  if (any(dupes)) {
    abort(c(
      "Duplicated field names",
      x = paste0(
        "The following field names were found more than once in the data: ",
        oxford_paste(names(.data)[dupes])
      )
    ))
  }

  structure(
    list(
      .data = if (inherits(.data, "Dataset")) {
        .data$clone()
      } else {
        InMemoryDataset$create(.data)
      },
      # selected_columns is a named list:
      # * contents are references/expressions pointing to the data
      # * names are the names they should be in the end (i.e. this
      #   records any renaming)
      selected_columns = make_field_refs(names(.data)),
      # filtered_rows will be an Expression
      filtered_rows = TRUE,
      # group_by_vars is a character vector of columns (as renamed)
      # in the data. They will be kept when data is pulled into R.
      group_by_vars = character(),
      # drop_empty_groups is a logical value indicating whether to drop
      # groups formed by factor levels that don't appear in the data. It
      # should be non-null only when the data is grouped.
      drop_empty_groups = NULL,
      # arrange_vars will be a list of expressions named by their associated
      # column names
      arrange_vars = list(),
      # arrange_desc will be a logical vector indicating the sort order for each
      # expression in arrange_vars (FALSE for ascending, TRUE for descending)
      arrange_desc = logical()
    ),
    class = "arrow_dplyr_query"
  )
}

make_field_refs <- function(field_names) {
  set_names(lapply(field_names, Expression$field_ref), field_names)
}

#' @export
print.arrow_dplyr_query <- function(x, ...) {
  schm <- x$.data$schema
  types <- map_chr(x$selected_columns, function(expr) {
    name <- expr$field_name
    if (nzchar(name)) {
      # Just a field_ref, so look up in the schema
      schm$GetFieldByName(name)$type$ToString()
    } else {
      # Expression, so get its type and append the expression
      paste0(
        expr$type(schm)$ToString(),
        " (", expr$ToString(), ")"
      )
    }
  })
  fields <- paste(names(types), types, sep = ": ", collapse = "\n")
  cat(class(x$.data)[1], " (query)\n", sep = "")
  cat(fields, "\n", sep = "")
  cat("\n")
  if (!isTRUE(x$filtered_rows)) {
    filter_string <- x$filtered_rows$ToString()
    cat("* Filter: ", filter_string, "\n", sep = "")
  }
  if (length(x$group_by_vars)) {
    cat("* Grouped by ", paste(x$group_by_vars, collapse = ", "), "\n", sep = "")
  }
  if (length(x$arrange_vars)) {
    arrange_strings <- map_chr(x$arrange_vars, function(x) x$ToString())
    cat(
      "* Sorted by ",
      paste(
        paste0(
          arrange_strings,
          " [", ifelse(x$arrange_desc, "desc", "asc"), "]"
        ),
        collapse = ", "
      ),
      "\n",
      sep = ""
    )
  }
  cat("See $.data for the source Arrow object\n")
  invisible(x)
}

# These are the names reflecting all select/rename, not what is in Arrow
#' @export
names.arrow_dplyr_query <- function(x) names(x$selected_columns)

#' @export
dim.arrow_dplyr_query <- function(x) {
  cols <- length(names(x))

  if (isTRUE(x$filtered)) {
    rows <- x$.data$num_rows
  } else {
    rows <- Scanner$create(x)$CountRows()
  }
  c(rows, cols)
}

#' @export
as.data.frame.arrow_dplyr_query <- function(x, row.names = NULL, optional = FALSE, ...) {
  collect.arrow_dplyr_query(x, as_data_frame = TRUE, ...)
}

#' @export
head.arrow_dplyr_query <- function(x, n = 6L, ...) {
  out <- head.Dataset(x, n, ...)
  restore_dplyr_features(out, x)
}

#' @export
tail.arrow_dplyr_query <- function(x, n = 6L, ...) {
  out <- tail.Dataset(x, n, ...)
  restore_dplyr_features(out, x)
}

#' @export
`[.arrow_dplyr_query` <- `[.Dataset`
# TODO: ^ should also probably restore_dplyr_features, and/or that should be moved down

ensure_group_vars <- function(x) {
  if (inherits(x, "arrow_dplyr_query")) {
    # Before pulling data from Arrow, make sure all group vars are in the projection
    gv <- set_names(setdiff(dplyr::group_vars(x), names(x)))
    if (length(gv)) {
      # Add them back
      x$selected_columns <- c(
        x$selected_columns,
        make_field_refs(gv)
      )
    }
  }
  x
}

ensure_arrange_vars <- function(x) {
  # The arrange() operation is not performed until later, because:
  # - It must be performed after mutate(), to enable sorting by new columns.
  # - It should be performed after filter() and select(), for efficiency.
  # However, we need users to be able to arrange() by columns and expressions
  # that are *not* returned in the query result. To enable this, we must
  # *temporarily* include these columns and expressions in the projection. We
  # use x$temp_columns to store these. Later, after the arrange() operation has
  # been performed, these are omitted from the result. This differs from the
  # columns in x$group_by_vars which *are* returned in the result.
  x$temp_columns <- x$arrange_vars[!names(x$arrange_vars) %in% names(x$selected_columns)]
  x
}

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
      df <- arrow_dplyr_query(df)
      df$group_by_vars <- query$group_by_vars
      df$drop_empty_groups <- query$drop_empty_groups
    }
  }
  df
}

# Helper to handle unsupported dplyr features
# * For Table/RecordBatch, we collect() and then call the dplyr method in R
# * For Dataset, we just error
abandon_ship <- function(call, .data, msg) {
  dplyr_fun_name <- sub("^(.*?)\\..*", "\\1", as.character(call[[1]]))
  if (query_on_dataset(.data)) {
    stop(msg, "\nCall collect() first to pull data into R.", call. = FALSE)
  }
  # else, collect and call dplyr method
  msg <- sub("\\n$", "", msg)
  warning(msg, "; pulling data into R", immediate. = TRUE, call. = FALSE)
  call$.data <- dplyr::collect(.data)
  call[[1]] <- get(dplyr_fun_name, envir = asNamespace("dplyr"))
  eval.parent(call, 2)
}

query_on_dataset <- function(x) !inherits(x$.data, "InMemoryDataset")
