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
  if (inherits(.data, "arrow_dplyr_query")) {
    return(.data)
  }
  structure(
    list(
      .data = .data$clone(),
      selected_columns = stats::setNames(names(.data), names(.data)),
      filtered_rows = TRUE,
      group_by_vars = character()
    ),
    class = "arrow_dplyr_query"
  )
}

select.arrow_dplyr_query <- function(.data, ...) {
  # This S3 method is registered on load if dplyr is present
  column_select(arrow_dplyr_query(.data), !!!enquos(...))
}
select.Dataset <- select.Table <- select.RecordBatch <- select.arrow_dplyr_query

#' @importFrom tidyselect vars_rename
rename.arrow_dplyr_query <- function(.data, ...) {
  # This S3 method is registered on load if dplyr is present
  column_select(arrow_dplyr_query(.data), !!!enquos(...), .FUN = vars_rename)
}
rename.Dataset <- rename.Table <- rename.RecordBatch <- rename.arrow_dplyr_query

column_select <- function(.data, ..., .FUN = vars_select) {
  out <- .FUN(names(.data$selected_columns), !!!enquos(...))
  # Make sure that the resulting selected columns map back to the original data
  # as in when there are multiple renaming steps
  .data$selected_columns <- stats::setNames(.data$selected_columns[out], names(out))

  renamed <- out[names(out) != out]
  if (length(renamed)) {
    # Massage group_by
    gbv <- .data$group_by_vars
    renamed_groups <- gbv %in% renamed
    gbv[renamed_groups] <- names(renamed)[match(gbv[renamed_groups], renamed)]
    .data$group_by_vars <- gbv
    # No need to massage filters because those contain pointers to Arrays
  }
  .data
}

filter.arrow_dplyr_query <- function(.data, ..., .preserve = FALSE) {
  # This S3 method is registered on load if dplyr is present
  # TODO something with the .preserve argument
  filts <- quos(...)
  if (length(filts) == 0) {
    # Nothing to do
    return(.data)
  }
  .data <- arrow_dplyr_query(.data)
  # Eval filters to generate Expressions with references to Arrays.
  filter_data <- env()
  data_is_dataset <- inherits(.data$.data, "Dataset")
  for (v in unique(unlist(lapply(filts, all.vars)))) {
    # Map any renamed vars to their name in the underlying Arrow schema
    if (!(v %in% names(.data$selected_columns))) {
      stop("object '", v, "' not found", call. = FALSE)
    }
    old_var_name <- .data$selected_columns[v]
    if (data_is_dataset) {
      # Make a FieldExpression
      this <- FieldExpression$create(old_var_name)
    } else {
      # Get the Array
      this <- .data$.data[[old_var_name]]
    }
    assign(v, this, envir = filter_data)
  }
  dm <- new_data_mask(filter_data)
  filters <- try(lapply(filts, function (f) {
    # This should yield an Expression
    eval_tidy(f, dm)
  }), silent = FALSE)
  # If that errored, bail out and collect(), with a warning
  # TODO: consider re-evaling with the as.vector Arrays and yielding logical vector
  if (inherits(filters, "try-error")) {
    # TODO: only show this in some debug mode?
    # TODO: if data_is_dataset, don't auto-collect?
    warning(
      "Filter expression not implemented in arrow, pulling data into R",
      immediate. = TRUE,
      call. = FALSE
    )
    return(dplyr::filter(dplyr::collect(.data), ...))
  }

  # filters is a list of Expressions. AND them together and return
  new_filter <- Reduce("&", filters)
  if (isTRUE(.data$filtered_rows)) {
    .data$filtered_rows <- new_filter
  } else {
    .data$filtered_rows <- .data$filtered_rows & new_filter
  }
  .data
}
filter.Dataset <- filter.Table <- filter.RecordBatch <- filter.arrow_dplyr_query

collect.arrow_dplyr_query <- function(x, ...) {
  # This S3 method is registered on load if dplyr is present
  colnames <- x$selected_columns
  # Be sure to retain any group_by vars
  gv <- setdiff(dplyr::group_vars(x), names(colnames))
  if (length(gv)) {
    colnames <- c(colnames, stats::setNames(gv, gv))
  }

  # Pull only the selected rows and cols into R
  if (inherits(x$.data, "Dataset")) {
    scanner_builder <- x$.data$NewScan()
    scanner_builder$UseThreads()
    scanner_builder$Project(colnames)
    if (!isTRUE(x$filtered_rows)) {
      scanner_builder$Filter(x$filtered_rows)
    }
    df <- as.data.frame(scanner_builder$Finish()$ToTable())
  } else {
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
collect.Dataset <- function(x, ...) stop("not implemented")

#' @importFrom tidyselect vars_pull
pull.arrow_dplyr_query <- function(.data, var = -1) {
  # This S3 method is registered on load if dplyr is present
  .data <- arrow_dplyr_query(.data)
  var <- vars_pull(names(.data$selected_columns), !!enquo(var))
  .data$selected_columns <- stats::setNames(.data$selected_columns[var], var)
  dplyr::collect(.data)[[1]]
}
pull.Dataset <- pull.Table <- pull.RecordBatch <- pull.arrow_dplyr_query

summarise.arrow_dplyr_query <- function(.data, ...) {
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
summarise.Dataset <- summarise.Table <- summarise.RecordBatch <- summarise.arrow_dplyr_query

group_by.arrow_dplyr_query <- function(.data, ..., add = FALSE) {
  # This S3 method is registered on load if dplyr is present
  .data <- arrow_dplyr_query(.data)
  .data$group_by_vars <- dplyr::group_by_prepare(.data, ..., add = add)$group_names
  .data
}
group_by.Dataset <- group_by.Table <- group_by.RecordBatch <- group_by.arrow_dplyr_query

# This S3 method is registered on load if dplyr is present
groups.arrow_dplyr_query <- function(x) syms(dplyr::group_vars(x))
groups.Dataset <- groups.Table <- groups.RecordBatch <- function(x) NULL

# This S3 method is registered on load if dplyr is present
group_vars.arrow_dplyr_query <- function(x) x$group_by_vars
group_vars.Dataset <- group_vars.Table <- group_vars.RecordBatch <- function(x) NULL

ungroup.arrow_dplyr_query <- function(x, ...) {
  # This S3 method is registered on load if dplyr is present
  x$group_by_vars <- character()
  x
}
ungroup.Dataset <- ungroup.Table <- ungroup.RecordBatch <- force

mutate.arrow_dplyr_query <- function(.data, ...) {
  # This S3 method is registered on load if dplyr is present
  dplyr::mutate(dplyr::collect(arrow_dplyr_query(.data)), ...)
}
mutate.Table <- mutate.RecordBatch <- mutate.arrow_dplyr_query
mutate.Dataset <- function(.data, ...) stop("not implemented")

arrange.arrow_dplyr_query <- function(.data, ...) {
  # This S3 method is registered on load if dplyr is present
  dplyr::arrange(dplyr::collect(arrow_dplyr_query(.data)), ...)
}
arrange.Table <- arrange.RecordBatch <- arrange.arrow_dplyr_query
arrange.Dataset <- function(.data, ...) stop("not implemented")
