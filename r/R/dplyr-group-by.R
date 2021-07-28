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
                                       add = .add,
                                       .drop = dplyr::group_by_drop_default(.data)) {
  .data <- arrow_dplyr_query(.data)
  new_groups <- enquos(...)
  # ... can contain expressions (i.e. can add (or rename?) columns) and so we
  # need to identify those and add them on to the query with mutate. Specifically,
  # we want to mark as new:
  #   * expressions (named or otherwise)
  #   * variables that have new names
  # All others (i.e. simple references to variables) should not be (re)-added
  new_group_ind <- map_lgl(new_groups, ~!(quo_name(.x) %in% names(.data)))
  named_group_ind <- map_lgl(names(new_groups), nzchar)
  new_groups <- new_groups[new_group_ind | named_group_ind]
  if (length(new_groups)) {
    # now either use the name that was given in ... or if that is "" then use the expr
    names(new_groups) <- imap_chr(new_groups, ~ ifelse(.y == "", quo_name(.x), .y))

    # Add them to the data
    .data <- dplyr::mutate(.data, !!!new_groups)
  }
  if (".add" %in% names(formals(dplyr::group_by))) {
    # dplyr >= 1.0
    gv <- dplyr::group_by_prepare(.data, ..., .add = .add)$group_names
  } else {
    gv <- dplyr::group_by_prepare(.data, ..., add = add)$group_names
  }
  .data$group_by_vars <- gv
  .data$drop_empty_groups <- ifelse(length(gv), .drop, dplyr::group_by_drop_default(.data))
  .data
}
group_by.Dataset <- group_by.ArrowTabular <- group_by.arrow_dplyr_query

groups.arrow_dplyr_query <- function(x) syms(dplyr::group_vars(x))
groups.Dataset <- groups.ArrowTabular <- function(x) NULL

group_vars.arrow_dplyr_query <- function(x) x$group_by_vars
group_vars.Dataset <- group_vars.ArrowTabular <- function(x) NULL

# the logical literal in the two functions below controls the default value of
# the .drop argument to group_by()
group_by_drop_default.arrow_dplyr_query <-
  function(.tbl) .tbl$drop_empty_groups %||% TRUE
group_by_drop_default.Dataset <- group_by_drop_default.ArrowTabular <-
  function(.tbl) TRUE

ungroup.arrow_dplyr_query <- function(x, ...) {
  x$group_by_vars <- character()
  x$drop_empty_groups <- NULL
  x
}
ungroup.Dataset <- ungroup.ArrowTabular <- force
