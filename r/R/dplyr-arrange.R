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

arrange.arrow_dplyr_query <- function(.data, ..., .by_group = FALSE) {
  call <- match.call()
  .data <- as_adq(.data)
  exprs <- expand_across(.data, quos(...))

  if (.by_group) {
    # when the data is is grouped and .by_group is TRUE, order the result by
    # the grouping columns first
    exprs <- c(quos(!!!dplyr::groups(.data)), exprs)
  }
  if (length(exprs) == 0) {
    # Nothing to do
    return(.data)
  }
  .data <- as_adq(.data)
  # find and remove any dplyr::desc() and tidy-eval
  # the arrange expressions inside an Arrow data_mask
  sorts <- vector("list", length(exprs))
  descs <- logical(0)
  mask <- arrow_mask(.data)
  for (i in seq_along(exprs)) {
    x <- find_and_remove_desc(exprs[[i]])
    exprs[[i]] <- x[["quos"]]
    sorts[[i]] <- arrow_eval(exprs[[i]], mask)
    names(sorts)[i] <- format_expr(exprs[[i]])
    if (inherits(sorts[[i]], "try-error")) {
      msg <- paste("Expression", names(sorts)[i], "not supported in Arrow")
      return(abandon_ship(call, .data, msg))
    }
    descs[i] <- x[["desc"]]
  }
  .data$arrange_vars <- c(sorts, .data$arrange_vars)
  .data$arrange_desc <- c(descs, .data$arrange_desc)
  .data
}
arrange.Dataset <- arrange.ArrowTabular <- arrange.RecordBatchReader <- arrange.arrow_dplyr_query

# Helper to handle desc() in arrange()
# * Takes a quosure as input
# * Returns a list with two elements:
#   1. The quosure with any wrapping parentheses and desc() removed
#   2. A logical value indicating whether desc() was found
# * Performs some other validation
find_and_remove_desc <- function(quosure) {
  expr <- quo_get_expr(quosure)
  descending <- FALSE
  if (length(all.vars(expr)) < 1L) {
    stop(
      "Expression in arrange() does not contain any field names: ",
      deparse(expr),
      call. = FALSE
    )
  }
  # Use a while loop to remove any number of nested pairs of enclosing
  # parentheses and any number of nested desc() calls. In the case of multiple
  # nested desc() calls, each one toggles the sort order.
  while (identical(typeof(expr), "language") && is.call(expr)) {
    if (identical(expr[[1]], quote(`(`))) {
      # remove enclosing parentheses
      expr <- expr[[2]]
    } else if (identical(expr[[1]], quote(desc)) || identical(expr[[1]], quote(dplyr::desc))) {
      # ensure desc() has only one argument (when an R expression is a function
      # call, length == 2 means it has exactly one argument)
      if (length(expr) > 2) {
        stop("desc() expects only one argument", call. = FALSE)
      }
      # remove desc() and toggle descending
      expr <- expr[[2]]
      descending <- !descending
    } else {
      break
    }
  }
  return(
    list(
      quos = quo_set_expr(quosure, expr),
      desc = descending
    )
  )
}
