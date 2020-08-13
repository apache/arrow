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
  structure(
    list(
      .data = .data$clone(),
      # selected_columns is a named character vector:
      # * vector contents are the names of the columns in the data
      # * vector names are the names they should be in the end (i.e. this
      #   records any renaming)
      selected_columns = set_names(names(.data)),
      # filtered_rows will be an Expression
      filtered_rows = TRUE,
      # group_by_vars is a character vector of columns (as renamed)
      # in the data. They will be kept when data is pulled into R.
      group_by_vars = character()
    ),
    class = "arrow_dplyr_query"
  )
}

#' @export
print.arrow_dplyr_query <- function(x, ...) {
  schm <- x$.data$schema
  cols <- x$selected_columns
  fields <- map_chr(cols, ~schm$GetFieldByName(.)$ToString())
  # Strip off the field names as they are in the dataset and add the renamed ones
  fields <- paste(names(cols), sub("^.*?: ", "", fields), sep = ": ", collapse = "\n")
  cat(class(x$.data)[1], " (query)\n", sep = "")
  cat(fields, "\n", sep = "")
  cat("\n")
  if (!isTRUE(x$filtered_rows)) {
    if (query_on_dataset(x)) {
      filter_string <- x$filtered_rows$ToString()
    } else {
      filter_string <- .format_array_expression(x$filtered_rows)
    }
    cat("* Filter: ", filter_string, "\n", sep = "")
  }
  if (length(x$group_by_vars)) {
    cat("* Grouped by ", paste(x$group_by_vars, collapse = ", "), "\n", sep = "")
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
  } else if (query_on_dataset(x)) {
    warning("Number of rows unknown; returning NA", call. = FALSE)
    # TODO: https://issues.apache.org/jira/browse/ARROW-9697
    rows <- NA_integer_
  } else {
    # Evaluate the filter expression to a BooleanArray and count
    rows <- as.integer(sum(eval_array_expression(x$filtered_rows), na.rm = TRUE))
  }
  c(rows, cols)
}

#' @export
as.data.frame.arrow_dplyr_query <- function(x, row.names = NULL, optional = FALSE, ...) {
  collect.arrow_dplyr_query(x, as_data_frame = TRUE, ...)
}

#' @export
head.arrow_dplyr_query <- function(x, n = 6L, ...) {
  if (query_on_dataset(x)) {
    head.Dataset(x, n, ...)
  } else {
    out <- collect.arrow_dplyr_query(x, as_data_frame = FALSE)
    if (inherits(out, "arrow_dplyr_query")) {
      out$.data <- head(out$.data, n)
    } else {
      out <- head(out, n)
    }
    out
  }
}

#' @export
tail.arrow_dplyr_query <- function(x, n = 6L, ...) {
  if (query_on_dataset(x)) {
    tail.Dataset(x, n, ...)
  } else {
    out <- collect.arrow_dplyr_query(x, as_data_frame = FALSE)
    if (inherits(out, "arrow_dplyr_query")) {
      out$.data <- tail(out$.data, n)
    } else {
      out <- tail(out, n)
    }
    out
  }
}

#' @export
`[.arrow_dplyr_query` <- function(x, i, j, ..., drop = FALSE) {
  if (query_on_dataset(x)) {
    `[.Dataset`(x, i, j, ..., drop = FALSE)
  } else {
    stop(
      "[ method not implemented for queries. Call 'collect(x, as_data_frame = FALSE)' first",
      call. = FALSE
    )
  }
}

# The following S3 methods are registered on load if dplyr is present
tbl_vars.arrow_dplyr_query <- function(x) names(x$selected_columns)

select.arrow_dplyr_query <- function(.data, ...) {
  column_select(arrow_dplyr_query(.data), !!!enquos(...))
}
select.Dataset <- select.Table <- select.RecordBatch <- select.arrow_dplyr_query

#' @importFrom tidyselect vars_rename
rename.arrow_dplyr_query <- function(.data, ...) {
  column_select(arrow_dplyr_query(.data), !!!enquos(...), .FUN = vars_rename)
}
rename.Dataset <- rename.Table <- rename.RecordBatch <- rename.arrow_dplyr_query

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

filter.arrow_dplyr_query <- function(.data, ..., .preserve = FALSE) {
  # TODO something with the .preserve argument
  filts <- quos(...)
  if (length(filts) == 0) {
    # Nothing to do
    return(.data)
  }

  .data <- arrow_dplyr_query(.data)
  # The filter() method works by evaluating the filters to generate Expressions
  # with references to Arrays (if .data is Table/RecordBatch) or Fields (if
  # .data is a Dataset).
  dm <- filter_mask(.data)
  filters <- lapply(filts, function (f) {
    # This should yield an Expression as long as the filter function(s) are
    # implemented in Arrow.
    tryCatch(eval_tidy(f, dm), error = function(e) {
      # Look for the cases where bad input was given, i.e. this would fail
      # in regular dplyr anyway, and let those raise those as errors;
      # else, for things not supported by Arrow return a "try-error",
      # which we'll handle differently
      msg <- conditionMessage(e)
      # TODO: internationalization?
      if (grepl("object '.*'.not.found", msg)) {
        stop(e)
      }
      if (grepl('could not find function ".*"', msg)) {
        stop(e)
      }
      invisible(structure(msg, class = "try-error", condition = e))
    })
  })
  bad_filters <- map_lgl(filters, ~inherits(., "try-error"))
  if (any(bad_filters)) {
    bads <- oxford_paste(map_chr(filts, as_label)[bad_filters], quote = FALSE)
    if (query_on_dataset(.data)) {
      # Abort. We don't want to auto-collect if this is a Dataset because that
      # could blow up, too big.
      stop(
        "Filter expression not supported for Arrow Datasets: ", bads,
        "\nCall collect() first to pull data into R.",
        call. = FALSE
      )
    } else {
      # TODO: only show this in some debug mode?
      warning(
        "Filter expression not implemented in Arrow: ", bads, "; pulling data into R",
        immediate. = TRUE,
        call. = FALSE
      )
      # Set any valid filters first, then collect and then apply the invalid ones in R
      .data <- set_filters(.data, filters[!bad_filters])
      return(dplyr::filter(dplyr::collect(.data), !!!filts[bad_filters]))
    }
  }

  set_filters(.data, filters)
}
filter.Dataset <- filter.Table <- filter.RecordBatch <- filter.arrow_dplyr_query

# Create a data mask for evaluating a filter expression
filter_mask <- function(.data) {
  f_env <- env()

  # Insert functions/operators and field references
  # TODO: define functions in env once, outside of this function
  # filter_env <- env(parent = if (data_is_dataset) function_env1 else function_env2)
  if (query_on_dataset(.data)) {
    comp_func <- function(operator) {
      force(operator)
      function(e1, e2) make_expression(operator, e1, e2)
    }
    var_binder <- function(x) Expression$field_ref(x)
  } else {
    comp_func <- function(operator) {
      force(operator)
      function(e1, e2) build_array_expression(operator, e1, e2)
    }
    var_binder <- function(x) .data$.data[[x]]
  }

  # First add the functions
  func_names <- set_names(names(.array_function_map))
  env_bind(f_env, !!!lapply(func_names, comp_func))
  # Then add the column references
  # Renaming is handled automatically by the named list
  env_bind(f_env, !!!lapply(.data$selected_columns, var_binder))
  new_data_mask(f_env)
}

set_filters <- function(.data, expressions) {
  # expressions is a list of Expressions. AND them together and set them on .data
  new_filter <- Reduce("&", expressions)
  if (isTRUE(.data$filtered_rows)) {
    # TRUE is default (i.e. no filter yet), so we don't need to & with it
    .data$filtered_rows <- new_filter
  } else {
    .data$filtered_rows <- .data$filtered_rows & new_filter
  }
  .data
}

collect.arrow_dplyr_query <- function(x, as_data_frame = TRUE, ...) {
  x <- ensure_group_vars(x)
  # Pull only the selected rows and cols into R
  if (query_on_dataset(x)) {
    # See dataset.R for Dataset and Scanner(Builder) classes
    df <- Scanner$create(x)$ToTable()
  } else {
    # This is a Table/RecordBatch. See record-batch.R for the [ method
    df <- x$.data[x$filtered_rows, x$selected_columns, keep_na = FALSE]
  }
  if (as_data_frame) {
    df <- as.data.frame(df)
  }
  restore_dplyr_features(df, x)
}
collect.Table <- as.data.frame.Table
collect.RecordBatch <- as.data.frame.RecordBatch
collect.Dataset <- function(x, ...) dplyr::collect(arrow_dplyr_query(x), ...)

ensure_group_vars <- function(x) {
  if (inherits(x, "arrow_dplyr_query")) {
    # Before pulling data from Arrow, make sure all group vars are in the projection
    gv <- set_names(setdiff(dplyr::group_vars(x), names(x)))
    x$selected_columns <- c(x$selected_columns, gv)
  }
  x
}

restore_dplyr_features <- function(df, query) {
  # An arrow_dplyr_query holds some attributes that Arrow doesn't know about
  # After calling collect(), make sure these features are carried over

  grouped <- length(query$group_by_vars) > 0
  renamed <- !identical(names(df), names(query))
  if (is.data.frame(df)) {
    # In case variables were renamed, apply those names
    if (renamed && ncol(df)) {
      names(df) <- names(query)
    }
    # Preserve groupings, if present
    if (grouped) {
      df <- dplyr::grouped_df(df, dplyr::group_vars(query))
    }
  } else if (grouped || renamed) {
    # This is a Table, via collect(as_data_frame = FALSE)
    df <- arrow_dplyr_query(df)
    names(df$selected_columns) <- names(query)
    df$group_by_vars <- query$group_by_vars
  }
  df
}

#' @importFrom tidyselect vars_pull
pull.arrow_dplyr_query <- function(.data, var = -1) {
  .data <- arrow_dplyr_query(.data)
  var <- vars_pull(names(.data), !!enquo(var))
  .data$selected_columns <- set_names(.data$selected_columns[var], var)
  dplyr::collect(.data)[[1]]
}
pull.Dataset <- pull.Table <- pull.RecordBatch <- pull.arrow_dplyr_query

summarise.arrow_dplyr_query <- function(.data, ...) {
  .data <- arrow_dplyr_query(.data)
  if (query_on_dataset(.data)) {
    not_implemented_for_dataset("summarize()")
  }
  # Only retain the columns we need to do our aggregations
  vars_to_keep <- unique(c(
    unlist(lapply(quos(...), all.vars)), # vars referenced in summarise
    dplyr::group_vars(.data)             # vars needed for grouping
  ))
  .data <- dplyr::select(.data, vars_to_keep)
  # TODO: determine whether work can be pushed down to Arrow
  dplyr::summarise(dplyr::collect(.data), ...)
}
summarise.Dataset <- summarise.Table <- summarise.RecordBatch <- summarise.arrow_dplyr_query

group_by.arrow_dplyr_query <- function(.data, ..., .add = FALSE, add = .add) {
  .data <- arrow_dplyr_query(.data)
  if (".add" %in% names(formals(dplyr::group_by))) {
    # dplyr >= 1.0
    gv <- dplyr::group_by_prepare(.data, ..., .add = .add)$group_names
  } else {
    gv <- dplyr::group_by_prepare(.data, ..., add = add)$group_names
  }
  .data$group_by_vars <- gv
  .data
}
group_by.Dataset <- group_by.Table <- group_by.RecordBatch <- group_by.arrow_dplyr_query

groups.arrow_dplyr_query <- function(x) syms(dplyr::group_vars(x))
groups.Dataset <- groups.Table <- groups.RecordBatch <- function(x) NULL

group_vars.arrow_dplyr_query <- function(x) x$group_by_vars
group_vars.Dataset <- group_vars.Table <- group_vars.RecordBatch <- function(x) NULL

ungroup.arrow_dplyr_query <- function(x, ...) {
  x$group_by_vars <- character()
  x
}
ungroup.Dataset <- ungroup.Table <- ungroup.RecordBatch <- force

mutate.arrow_dplyr_query <- function(.data, ...) {
  .data <- arrow_dplyr_query(.data)
  if (query_on_dataset(.data)) {
    not_implemented_for_dataset("mutate()")
  }
  # TODO: see if we can defer evaluating the expressions and not collect here.
  # It's different from filters (as currently implemented) because the basic
  # vector transformation functions aren't yet implemented in Arrow C++.
  dplyr::mutate(dplyr::collect(.data), ...)
}
mutate.Dataset <- mutate.Table <- mutate.RecordBatch <- mutate.arrow_dplyr_query
# TODO: add transmute() that does what summarise() does (select only the vars we need)

arrange.arrow_dplyr_query <- function(.data, ...) {
  .data <- arrow_dplyr_query(.data)
  if (query_on_dataset(.data)) {
    not_implemented_for_dataset("arrange()")
  }

  dplyr::arrange(dplyr::collect(.data), ...)
}
arrange.Dataset <- arrange.Table <- arrange.RecordBatch <- arrange.arrow_dplyr_query

query_on_dataset <- function(x) inherits(x$.data, "Dataset")

not_implemented_for_dataset <- function(method) {
  stop(
    method, " is not currently implemented for Arrow Datasets. ",
    "Call collect() first to pull data into R.",
    call. = FALSE
  )
}
