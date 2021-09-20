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

distinct.arrow_dplyr_query <- function(.data, ..., .keep_all = FALSE) {
  if (.keep_all == TRUE) {
    # After ARROW-13993 is merged, we can implement this (ARROW-14045)
    arrow_not_supported("`distinct()` with `.keep_all = TRUE`")
  }

  distinct_groups <- ensure_named_exprs(quos(...))

  gv <- dplyr::group_vars(.data)

  # Get ordering to use when returning data
  # We need to do this as `data %>% group_by() %>% summarise()` returns cols in
  # the order supplied, whereas distinct() returns cols in dataset order.
  if (length(distinct_groups)) {
    cols_in_order <- intersect(
      unique(c(names(.data), names(distinct_groups))),
      unique(c(names(distinct_groups), gv))
    )
  } else {
    cols_in_order <- names(.data)
  }

  # This works as distinct(data, x, y) == summarise(group_by(data, x, y))
  if (length(distinct_groups)) {
    .data <- dplyr::group_by(.data, !!!distinct_groups, .add = TRUE)
  } else {
    # If there are no cols supplied, group by everything so that
    # later call to `summarise()`` returns everything and not empty table
    .data <- dplyr::group_by(.data, !!!syms(names(.data)), .add = TRUE)
  }

  .data <- dplyr::summarize(.data, .groups = "drop")

  # If there were no vars supplied to distinct() but there were vars supplied
  # to group_by, we need to restore grouping which we removed earlier when
  # grouping by everything
  if (length(gv)) {
    .data$group_by_vars <- gv
  }

  # If there are group_by expressions or expressions supplied to distinct(),
  # we need to reorder the selected columns to return them in the correct order
  # Select the columns to return in the correct order
  ordered_selected_cols <- intersect(
    cols_in_order,
    names(.data$selected_columns)
  )
  .data$selected_columns <- .data$selected_columns[ordered_selected_cols]

  .data
}

distinct.Dataset <- distinct.ArrowTabular <- distinct.arrow_dplyr_query
