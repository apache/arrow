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

group_by.arrow_dplyr_query <- function(.data,
                                       ...,
                                       .add = FALSE,
                                       add = NULL,
                                       .drop = dplyr::group_by_drop_default(.data)) {
  if (!missing(add)) {
    .Deprecated(
      msg = paste(
        "The `add` argument of `group_by()` is deprecated.",
        "Please use the `.add` argument instead."
      )
    )
    .add <- add
  }

  .data <- as_adq(.data)
  expression_list <- expand_across(.data, quos(...))
  named_expression_list <- ensure_named_exprs(expression_list)

  # Set up group names
  gbp <- dplyr::group_by_prepare(.data, !!!expression_list, .add = .add)

  # Add them all (or update them) to the .data via. In theory
  # one could calculate which variables do or do not need to be added via a
  # complex combination of the expression names, whether they are or are not
  # a symbol, and/or whether they currently exist in .data. Instead, we just
  # put them all into a mutate().
  existing_groups <- dplyr::groups(gbp$data)
  names(existing_groups) <- dplyr::group_vars(gbp$data)
  final_groups <- c(unclass(named_expression_list), unclass(existing_groups))[gbp$group_names]
  .data <- dplyr::mutate(.data, !!!final_groups)

  .data$group_by_vars <- gbp$group_names
  .data$drop_empty_groups <- ifelse(length(gbp$group_names), .drop, dplyr::group_by_drop_default(.data))
  .data
}
group_by.Dataset <- group_by.ArrowTabular <- group_by.RecordBatchReader <- group_by.arrow_dplyr_query

groups.arrow_dplyr_query <- function(x) syms(dplyr::group_vars(x))
groups.Dataset <- groups.ArrowTabular <- groups.RecordBatchReader <- groups.arrow_dplyr_query

group_vars.arrow_dplyr_query <- function(x) x$group_by_vars
group_vars.Dataset <- function(x) character()
group_vars.RecordBatchReader <- function(x) character()
group_vars.ArrowTabular <- function(x) {
  x$metadata$r$attributes$.group_vars %||% character()
}

# the logical literal in the two functions below controls the default value of
# the .drop argument to group_by()
group_by_drop_default.arrow_dplyr_query <- function(.tbl) {
  .tbl$drop_empty_groups %||% TRUE
}
group_by_drop_default.ArrowTabular <- function(.tbl) {
  .tbl$metadata$r$attributes$.group_by_drop %||% TRUE
}
group_by_drop_default.Dataset <- group_by_drop_default.RecordBatchReader <-
  function(.tbl) TRUE

ungroup.arrow_dplyr_query <- function(x, ...) {
  x$group_by_vars <- character()
  x$drop_empty_groups <- NULL
  x
}
ungroup.Dataset <- ungroup.RecordBatchReader <- force
ungroup.ArrowTabular <- function(x) {
  set_group_attributes(x, NULL, NULL)
}

# Function to call after evaluating a query (as_arrow_table()) to add back any
# group attributes to the Schema metadata. Or to remove them, pass NULL.
set_group_attributes <- function(tab, group_vars, .drop) {
  # dplyr::group_vars() returns character(0)
  # so passing NULL means unset (ungroup)
  if (is.null(group_vars) || length(group_vars)) {
    # Since accessing schema metadata does some work, only overwrite if needed
    new_atts <- old_atts <- tab$metadata$r$attributes %||% list()
    new_atts[[".group_vars"]] <- group_vars
    new_atts[[".group_by_drop"]] <- .drop
    if (!identical(new_atts, old_atts)) {
      tab$metadata$r$attributes <- new_atts
    }
  }
  tab
}
