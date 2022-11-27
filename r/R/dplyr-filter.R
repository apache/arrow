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

filter.arrow_dplyr_query <- function(.data, ..., .preserve = FALSE) {
  # TODO something with the .preserve argument
  filts <- expand_across(.data, quos(...))
  if (length(filts) == 0) {
    # Nothing to do
    return(.data)
  }

  .data <- as_adq(.data)
  # tidy-eval the filter expressions inside an Arrow data_mask
  filters <- lapply(filts, arrow_eval, arrow_mask(.data))
  bad_filters <- map_lgl(filters, ~ inherits(., "try-error"))
  if (any(bad_filters)) {
    # This is similar to abandon_ship() except that the filter eval is
    # vectorized, and we apply filters that _did_ work before abandoning ship
    # with the rest
    expr_labs <- map_chr(filts[bad_filters], format_expr)
    if (query_on_dataset(.data)) {
      # Abort. We don't want to auto-collect if this is a Dataset because that
      # could blow up, too big.
      stop(
        "Filter expression not supported for Arrow Datasets: ",
        oxford_paste(expr_labs, quote = FALSE),
        "\nCall collect() first to pull data into R.",
        call. = FALSE
      )
    } else {
      arrow_errors <- map2_chr(
        filters[bad_filters], expr_labs,
        handle_arrow_not_supported
      )
      if (length(arrow_errors) == 1) {
        msg <- paste0(arrow_errors, "; ")
      } else {
        msg <- paste0("* ", arrow_errors, "\n", collapse = "")
      }
      warning(
        msg, "pulling data into R",
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
filter.Dataset <- filter.ArrowTabular <- filter.RecordBatchReader <- filter.arrow_dplyr_query

set_filters <- function(.data, expressions) {
  if (length(expressions)) {
    if (is_list_of(expressions, "Expression")) {
      # expressions is a list of Expressions. AND them together and set them on .data
      new_filter <- Reduce("&", expressions)
    } else if (inherits(expressions, "Expression")) {
      new_filter <- expressions
    } else {
      stop("filter expressions must be either an expression or a list of expressions", call. = FALSE)
    }

    if (isTRUE(.data$filtered_rows)) {
      # TRUE is default (i.e. no filter yet), so we don't need to & with it
      .data$filtered_rows <- new_filter
    } else {
      .data$filtered_rows <- .data$filtered_rows & new_filter
    }
  }
  .data
}
