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

count.arrow_dplyr_query <- function(x, ..., wt = NULL, sort = FALSE, name = NULL) {
  if (!missing(...)) {
    out <- dplyr::group_by(x, ..., .add = TRUE)
  } else {
    out <- x
  }
  out <- dplyr::tally(out, wt = {{ wt }}, sort = sort, name = name)

  gv <- dplyr::group_vars(x)
  if (is_empty(gv)) {
    out <- dplyr::ungroup(out)
  } else {
    # Restore original group vars
    out$group_by_vars <- gv
  }
  out
}

count.Dataset <- count.ArrowTabular <- count.RecordBatchReader <- count.arrow_dplyr_query

#' @importFrom rlang sym :=
tally.arrow_dplyr_query <- function(x, wt = NULL, sort = FALSE, name = NULL) {
  name <- check_n_name(name, dplyr::group_vars(x))

  if (quo_is_null(enquo(wt))) {
    out <- dplyr::summarize(x, !!name := n())
  } else {
    out <- dplyr::summarize(x, !!name := sum({{ wt }}, na.rm = TRUE))
  }

  if (sort) {
    dplyr::arrange(out, desc(!!sym(name)))
  } else {
    out
  }
}

tally.Dataset <- tally.ArrowTabular <- tally.RecordBatchReader <- tally.arrow_dplyr_query

# we don't want to depend on dplyr, but we refrence these above
utils::globalVariables(c("n", "desc"))

check_n_name <- function(name,
                         vars,
                         call = caller_env()) {
  if (is.null(name)) {
    name <- n_name(vars)

    if (name != "n") {
      inform(c(
        glue("Storing counts in `{name}`, as `n` already present in input"),
        i = "Use `name = \"new_name\"` to pick a new name."
      ))
    }
  } else if (!is_string(name)) {
    abort("`name` must be a string or `NULL`.", call = call)
  }

  name
}

n_name <- function(x) {
  name <- "n"

  while (name %in% x) {
    name <- paste0("n", name)
  }

  name
}
