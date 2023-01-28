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
  # An arrow_dplyr_query can contain another arrow_dplyr_query in .data

  supported <- c(
    "Dataset", "RecordBatch", "RecordBatchReader",
    "Table", "arrow_dplyr_query", "data.frame"
  )
  if (!inherits(.data, supported)) {
    stop(
      "You must supply a ",
      oxford_paste(supported, "or", quote = FALSE),
      ", not an object of type ",
      deparse(class(.data)),
      call. = FALSE
    )
  }

  gv <- tryCatch(
    # If dplyr is not available, or if the input doesn't have a group_vars
    # method, assume no group vars
    dplyr::group_vars(.data),
    error = function(e) character()
  )

  if (inherits(.data, "data.frame")) {
    .data <- Table$create(.data)
  }
  # ARROW-17737: If .data is a Table, remove groups from metadata
  # (we've already grabbed the groups above)
  if (inherits(.data, "ArrowTabular")) {
    .data <- ungroup.ArrowTabular(.data)
  }

  # Evaluating expressions on a dataset with duplicated fieldnames will error
  dupes <- duplicated(names(.data))
  if (any(dupes)) {
    abort(c(
      "Field names must be unique.",
      x = paste0(
        "The following field names were found more than once in the data: ",
        oxford_paste(names(.data)[dupes])
      )
    ))
  }
  structure(
    list(
      .data = .data,
      # selected_columns is a named list:
      # * contents are references/expressions pointing to the data
      # * names are the names they should be in the end (i.e. this
      #   records any renaming)
      selected_columns = make_field_refs(names(.data$schema)),
      # filtered_rows will be an Expression
      filtered_rows = TRUE,
      # group_by_vars is a character vector of columns (as renamed)
      # in the data. They will be kept when data is pulled into R.
      group_by_vars = gv,
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

# The only difference between `arrow_dplyr_query()` and `as_adq()` is that if
# `.data` is already an `arrow_dplyr_query`, `as_adq()`, will return it as is, but
# `arrow_dplyr_query()` will nest it inside a new `arrow_dplyr_query`. The only
# place where `arrow_dplyr_query()` should be called directly is inside
# `collapse()` methods; everywhere else, call `as_adq()`.
as_adq <- function(.data) {
  # For most dplyr methods,
  # method.Table == method.RecordBatch == method.Dataset == method.arrow_dplyr_query
  # This works because the functions all pass .data through as_adq()
  if (inherits(.data, "arrow_dplyr_query")) {
    return(.data)
  }
  arrow_dplyr_query(.data)
}

make_field_refs <- function(field_names) {
  set_names(lapply(field_names, Expression$field_ref), field_names)
}

#' @export
print.arrow_dplyr_query <- function(x, ...) {
  schm <- x$.data$schema
  # If we are using this augmented field, it won't be in the schema
  schm[["__filename"]] <- string()

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
  cat(class(source_data(x))[1], " (query)\n", sep = "")
  cat(fields, "\n", sep = "")
  cat("\n")
  if (length(x$aggregations)) {
    cat("* Aggregations:\n")
    aggs <- paste0(names(x$aggregations), ": ", map_chr(x$aggregations, format_aggregation), collapse = "\n")
    cat(aggs, "\n", sep = "")
  }
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

  if (is_collapsed(x)) {
    # Don't evaluate just for nrow
    rows <- NA_integer_
  } else if (isTRUE(x$filtered_rows)) {
    rows <- x$.data$num_rows
  } else if (query_on_dataset(x)) {
    # TODO: do this with an ExecPlan instead of Scanner (after ARROW-12311)?
    # See also https://github.com/apache/arrow/pull/12533/files#r818129459
    rows <- Scanner$create(x)$CountRows()
  } else {
    # Query on in-memory Table, so evaluate the filter
    # Don't need any columns
    x <- select.arrow_dplyr_query(x, NULL)
    rows <- nrow(as_arrow_table(x))
  }
  c(rows, cols)
}

#' @export
unique.arrow_dplyr_query <- function(x, incomparables = FALSE, fromLast = FALSE, ...) {
  if (isTRUE(incomparables)) {
    arrow_not_supported("`unique()` with `incomparables = TRUE`")
  }

  if (fromLast == TRUE) {
    arrow_not_supported("`unique()` with `fromLast = TRUE`")
  }

  dplyr::distinct(x)
}

#' @export
unique.Dataset <- unique.arrow_dplyr_query
#' @export
unique.ArrowTabular <- unique.arrow_dplyr_query
#' @export
unique.RecordBatchReader <- unique.arrow_dplyr_query


#' @export
as.data.frame.arrow_dplyr_query <- function(x, row.names = NULL, optional = FALSE, ...) {
  collect.arrow_dplyr_query(x, as_data_frame = TRUE, ...)
}

#' @export
head.arrow_dplyr_query <- function(x, n = 6L, ...) {
  assert_is(n, c("numeric", "integer"))
  assert_that(length(n) == 1)
  if (!is.integer(n)) {
    n <- floor(n)
  }
  x$head <- n
  collapse.arrow_dplyr_query(x)
}

#' @export
tail.arrow_dplyr_query <- function(x, n = 6L, ...) {
  assert_is(n, c("numeric", "integer"))
  assert_that(length(n) == 1)
  if (!is.integer(n)) {
    n <- floor(n)
  }
  x$tail <- n
  collapse.arrow_dplyr_query(x)
}

#' @export
`[.arrow_dplyr_query` <- function(x, i, j, ..., drop = FALSE) {
  x <- ensure_group_vars(x)
  if (nargs() == 2L) {
    # List-like column extraction (x[i])
    return(x[, i])
  }
  if (!missing(j)) {
    x <- select.arrow_dplyr_query(x, all_of(j))
  }

  if (!missing(i)) {
    out <- take_dataset_rows(x, i)
    x <- set_group_attributes(
      out,
      dplyr::group_vars(x),
      dplyr::group_by_drop_default(x)
    )
  }
  x
}

#' Show the details of an Arrow Execution Plan
#'
#' This is a function which gives more details about the logical query plan
#' that will be executed when evaluating an `arrow_dplyr_query` object.
#' It calls the C++ `ExecPlan` object's print method.
#' Functionally, it is similar to `dplyr::explain()`. This function is used as
#' the `dplyr::explain()` and `dplyr::show_query()` methods.
#'
#' @param x an `arrow_dplyr_query` to print the `ExecPlan` for.
#'
#' @return `x`, invisibly.
#' @export
#'
#' @examplesIf arrow_with_dataset() && requireNamespace("dplyr", quietly = TRUE)
#' library(dplyr)
#' mtcars %>%
#'   arrow_table() %>%
#'   filter(mpg > 20) %>%
#'   mutate(x = gear / carb) %>%
#'   show_exec_plan()
show_exec_plan <- function(x) {
  adq <- as_adq(x)

  # do not show the plan if we have a nested query (as this will force the
  # evaluation of the inner query/queries)
  # TODO see if we can remove after ARROW-16628
  if (is_collapsed(x) && has_head_tail(x$.data)) {
    warn("The `ExecPlan` cannot be printed for a nested query.")
    return(invisible(x))
  }

  result <- as_record_batch_reader(adq)
  plan <- result$Plan()
  on.exit({
    plan$.unsafe_delete()
    result$.unsafe_delete()
  })

  cat(plan$ToString())

  invisible(x)
}

show_query.arrow_dplyr_query <- function(x, ...) {
  show_exec_plan(x)
}

show_query.Dataset <- show_query.ArrowTabular <- show_query.RecordBatchReader <- show_query.arrow_dplyr_query

explain.arrow_dplyr_query <- function(x, ...) {
  show_exec_plan(x)
}

explain.Dataset <- explain.ArrowTabular <- explain.RecordBatchReader <- explain.arrow_dplyr_query

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

# Helper to handle unsupported dplyr features
# * For Table/RecordBatch, we collect() and then call the dplyr method in R
# * For Dataset, we just error
abandon_ship <- function(call, .data, msg) {
  msg <- trimws(msg)
  dplyr_fun_name <- sub("^(.*?)\\..*", "\\1", as.character(call[[1]]))
  if (query_on_dataset(.data)) {
    stop(msg, "\nCall collect() first to pull data into R.", call. = FALSE)
  }
  # else, collect and call dplyr method
  warning(msg, "; pulling data into R", immediate. = TRUE, call. = FALSE)
  call$.data <- dplyr::collect(.data)
  call[[1]] <- get(dplyr_fun_name, envir = asNamespace("dplyr"))
  eval.parent(call, 2)
}

query_on_dataset <- function(x) {
  any(map_lgl(all_sources(x), ~ inherits(., c("Dataset", "RecordBatchReader"))))
}

source_data <- function(x) {
  if (!inherits(x, "arrow_dplyr_query")) {
    x
  } else if (is_collapsed(x)) {
    source_data(x$.data)
  } else {
    x$.data
  }
}

all_sources <- function(x) {
  if (is.null(x)) {
    x
  } else if (!inherits(x, "arrow_dplyr_query")) {
    list(x)
  } else {
    c(
      all_sources(x$.data),
      all_sources(x$join$right_data),
      all_sources(x$union_all$right_data)
    )
  }
}

query_can_stream <- function(x) {
  # Queries that just select/filter/mutate can stream:
  # you can take head() without evaluating over the whole dataset
  if (inherits(x, "arrow_dplyr_query")) {
    # Aggregations require all of the data
    is.null(x$aggregations) &&
      # Sorting does too
      length(x$arrange_vars) == 0 &&
      # Joins are ok as long as the right-side data is in memory
      # (we have to hash the whole dataset to join it)
      !query_on_dataset(x$join$right_data) &&
      # But need to check that this non-dataset join can stream
      query_can_stream(x$join$right_data) &&
      # Also check that any unioned datasets also can stream
      query_can_stream(x$union_all$right_data) &&
      # Recursively check any queries that have been collapsed
      query_can_stream(x$.data)
  } else {
    # Not a query, so it must be a Table/Dataset (or NULL)
    # Note that if you have a RecordBatchReader, you *can* stream,
    # but the reader is consumed. If that's a problem, you should check
    # for RBRs outside of this function.
    TRUE
  }
}

is_collapsed <- function(x) inherits(x$.data, "arrow_dplyr_query")

has_head_tail <- function(x) {
  !is.null(x$head) || !is.null(x$tail) || (is_collapsed(x) && has_head_tail(x$.data))
}
