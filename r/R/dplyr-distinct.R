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
    # After ARROW-13767 is merged, we can implement this via e.g.
    # iris %>% group_by(Species) %>% slice(1) %>% ungroup()
    abort("`distinct()` with `keep_all = TRUE` argument not supported in Arrow")
  }

  distinct_groups <- quos(...)

  # Get grouping in the data as the call to summarize() will remove it
  gv <- dplyr::group_vars(.data)

  browser()

  vars_to_group <- unique(c(
    # THIS IS WRONG AS IT STRIPS OUT EXPRESSIONS
    unlist(lapply(distinct_groups, all.vars)),
    gv
  ))

  # Ensure vars are in the same order they are in the dataset
  ordered_vars_to_group <- intersect(names(.data), vars_to_group)

  if (length(vars_to_group) == 0) {
    return(.data)
  }

  .data <- dplyr::group_by(.data, !!!syms(ordered_vars_to_group))

  .data <- dplyr::summarize(.data)

  # Add back in any grouping which existed in the data previously
  if (length(gv) > 0) {
    .data$group_by_vars <- gv
  }


  # # Need to deal with naming here
  # old_vars <- names(.data$selected_columns)
  # # Note that this is names(exprs) not names(results):
  # # if results$new_var is NULL, that means we are supposed to remove it
  # new_vars <- names(distinct_groups)
  #
  # # Assign the new columns into the .data$selected_columns
  # for (new_var in new_vars) {
  #   .data$selected_columns[[new_var]] <- results[[new_var]]
  # }


  .data
}

distinct.Dataset <- distinct.ArrowTabular <- distinct.arrow_dplyr_query
