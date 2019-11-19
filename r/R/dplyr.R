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

#' @include record-batch.R
#' @include table.R

arrow_dplyr_query <- function(.data) {
  # An arrow_dplyr_query is a container for an Arrow data object (Table,
  # RecordBatch, or Dataset) and the state of the user's dplyr query--things
  # like selected colums, filters, and group vars.

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
      selected_columns = stats::setNames(names(.data), names(.data)),
      # filtered_rows will be a ComparisonExpression
      filtered_rows = TRUE,
      # group_by_vars is a character vector of columns (as renamed)
      # in the data. They will be kept when data is pulled into R.
      group_by_vars = character()
    ),
    class = "arrow_dplyr_query"
  )
}

# These are the names reflecting all select/rename, not what is in Arrow
names.arrow_dplyr_query <- function(x) names(x$selected_columns)

# The following S3 methods are registered on load if dplyr is present
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
  .data$selected_columns <- stats::setNames(.data$selected_columns[out], names(out))

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
  data_is_dataset <- inherits(.data$.data, "Dataset")

  # The filter() method works by evaluating the filters to generate Expressions
  # with references to Arrays (if .data is Table/RecordBatch) or Fields (if
  # .data is a Dataset).
  filter_data <- env()
  for (v in unique(unlist(lapply(filts, all.vars)))) {
    if (!(v %in% names(.data))) {
      stop("object '", v, "' not found", call. = FALSE)
    }
    # Map any renamed vars to their name in the underlying Arrow schema
    old_var_name <- .data$selected_columns[v]
    if (data_is_dataset) {
      # Make a FieldExpression
      this <- FieldExpression$create(old_var_name)
    } else {
      # Get the (reference to the) Array
      this <- .data$.data[[old_var_name]]
    }
    assign(v, this, envir = filter_data)
  }
  # Add %in% function since we can't define that as a field/array method naturally
  # (it's not a generic)
  if (data_is_dataset) {
    assign("%in%", InExpression$create, envir = filter_data)
  } else {
    assign("%in%", function(x, table) array_expression("%in%", x, table), envir = filter_data)
  }
  dm <- new_data_mask(filter_data)
  filters <- lapply(filts, function (f) {
    # This should yield an Expression as long as the filter function(s) are
    # implemented in Arrow.
    try(eval_tidy(f, dm), silent = TRUE)
  })

  bad_filters <- map_lgl(filters, ~inherits(., "try-error"))
  if (any(bad_filters)) {
    # TODO: consider ways not to bail out here, e.g. defer evaluating the
    # unsupported filters until after the data has been pulled to R.
    # See https://issues.apache.org/jira/browse/ARROW-7095
    bads <- oxford_paste(map_chr(filts, as_label)[bad_filters], quote = FALSE)
    if (data_is_dataset) {
      # Abort. We don't want to auto-collect if this is a Dataset because that
      # could blow up, too big.
      stop(
        "Filter expression not supported for Arrow Datasets: ", bads,
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

collect.arrow_dplyr_query <- function(x, ...) {
  colnames <- x$selected_columns
  # Be sure to retain any group_by vars
  gv <- setdiff(dplyr::group_vars(x), names(colnames))
  if (length(gv)) {
    colnames <- c(colnames, stats::setNames(gv, gv))
  }

  # Pull only the selected rows and cols into R
  if (inherits(x$.data, "Dataset")) {
    # See dataset.R for Dataset and Scanner(Builder) classes
    scanner_builder <- x$.data$NewScan()
    scanner_builder$UseThreads()
    scanner_builder$Project(colnames)
    if (!isTRUE(x$filtered_rows)) {
      scanner_builder$Filter(x$filtered_rows)
    }
    df <- as.data.frame(scanner_builder$Finish()$ToTable())
  } else {
    # This is a Table/RecordBatch. See record-batch.R for the [ method
    df <- as.data.frame(x$.data[x$filtered_rows, colnames])
  }
  # In case variables were renamed, apply those names
  names(df) <- names(colnames)

  # Preserve groupings, if present
  if (length(x$group_by_vars)) {
    df <- dplyr::grouped_df(df, dplyr::groups(x))
  }
  df
}
collect.Table <- as.data.frame.Table
collect.RecordBatch <- as.data.frame.RecordBatch
# We probably don't want someone to try to pull a big multi-file Dataset into
# R without first filtering/selecting
collect.Dataset <- function(x, ...) stop("not implemented")

#' @importFrom tidyselect vars_pull
pull.arrow_dplyr_query <- function(.data, var = -1) {
  .data <- arrow_dplyr_query(.data)
  var <- vars_pull(names(.data), !!enquo(var))
  .data$selected_columns <- stats::setNames(.data$selected_columns[var], var)
  dplyr::collect(.data)[[1]]
}
pull.Dataset <- pull.Table <- pull.RecordBatch <- pull.arrow_dplyr_query

summarise.arrow_dplyr_query <- function(.data, ...) {
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

group_by.arrow_dplyr_query <- function(.data, ..., add = FALSE) {
  .data <- arrow_dplyr_query(.data)
  .data$group_by_vars <- dplyr::group_by_prepare(.data, ..., add = add)$group_names
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
  # TODO: see if we can defer evaluating the expressions and not collect here.
  # It's different from filters (as currently implemented) because the basic
  # vector transformation functions aren't yet implemented in Arrow C++.
  dplyr::mutate(dplyr::collect(arrow_dplyr_query(.data)), ...)
}
mutate.Table <- mutate.RecordBatch <- mutate.arrow_dplyr_query
mutate.Dataset <- function(.data, ...) stop("not implemented")
# transmute() "just works" because it calls mutate() internally
# TODO: add transmute() that does what summarise() does (select only the vars we need)

arrange.arrow_dplyr_query <- function(.data, ...) {
  dplyr::arrange(dplyr::collect(arrow_dplyr_query(.data)), ...)
}
arrange.Table <- arrange.RecordBatch <- arrange.arrow_dplyr_query
arrange.Dataset <- function(.data, ...) stop("not implemented")
