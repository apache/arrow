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

apply_filter_impl <- function(
  .data,
  ...,
  .by = NULL,
  .preserve = FALSE,
  negate = FALSE
) {
  # TODO something with the .preserve argument
  out <- as_adq(.data)

  by <- compute_by({{ .by }}, out, by_arg = ".by", data_arg = ".data")

  if (by$from_by) {
    out$group_by_vars <- by$names
  }

  expanded_filters <- expand_across(out, quos(...))
  if (length(expanded_filters) == 0) {
    # Nothing to do
    return(as_adq(.data))
  }

  # tidy-eval the filter expressions inside an Arrow data_mask
  mask <- arrow_mask(out)

  if (isTRUE(negate)) {
    # filter_out(): combine all predicates with &, then negate
    combined <- NULL

    for (expr in expanded_filters) {
      filt <- arrow_eval(expr, mask)

      if (length(mask$.aggregations)) {
        # dplyr lets you filter on e.g. x < mean(x), but we haven't implemented it.
        # But we could, the same way it works in mutate() via join, if someone asks.
        # Until then, just error.
        arrow_not_supported(
          .actual_msg = "Expression not supported in filter_out() in Arrow",
          call = expr
        )
      }

      if (is_list_of(filt, "Expression")) {
        filt <- Reduce("&", filt)
      }

      combined <- if (is.null(combined)) filt else (combined & filt)
    }

    out <- set_filters(out, combined, negate = TRUE)
  } else {
    # filter(): apply each predicate sequentially
    for (expr in expanded_filters) {
      filt <- arrow_eval(expr, mask)

      if (length(mask$.aggregations)) {
        # dplyr lets you filter on e.g. x < mean(x), but we haven't implemented it.
        # But we could, the same way it works in mutate() via join, if someone asks.
        # Until then, just error.
        arrow_not_supported(
          .actual_msg = "Expression not supported in filter() in Arrow",
          call = expr
        )
      }

      out <- set_filters(out, filt, negate = FALSE)
    }
  }

  if (by$from_by) {
    out$group_by_vars <- character()
  }

  out
}

filter.arrow_dplyr_query <- function(
  .data,
  ...,
  .by = NULL,
  .preserve = FALSE
) {
  try_arrow_dplyr({
    apply_filter_impl(
      .data,
      ...,
      .by = {{ .by }},
      .preserve = .preserve,
      negate = FALSE
    )
  })
}
filter.Dataset <- filter.ArrowTabular <- filter.RecordBatchReader <- filter.arrow_dplyr_query

filter_out.arrow_dplyr_query <- function(
  .data,
  ...,
  .by = NULL,
  .preserve = FALSE
) {
  try_arrow_dplyr({
    apply_filter_impl(
      .data,
      ...,
      .by = {{ .by }},
      .preserve = .preserve,
      negate = TRUE
    )
  })
}
filter_out.Dataset <- filter_out.ArrowTabular <- filter_out.RecordBatchReader <- filter_out.arrow_dplyr_query

set_filters <- function(.data, expressions, negate = FALSE) {
  if (length(expressions)) {
    if (is_list_of(expressions, "Expression")) {
      # expressions is a list of Expressions. AND them together and set them on .data
      new_filter <- Reduce("&", expressions)
    } else if (inherits(expressions, "Expression")) {
      new_filter <- expressions
    } else {
      stop(
        "filter expressions must be either an expression or a list of expressions",
        call. = FALSE
      )
    }

    if (isTRUE(negate)) {
      # dplyr::filter_out() semantics: drop rows where predicate is TRUE;
      # keep rows where predicate is FALSE or NA.
      new_filter <- (!new_filter) | is.na(new_filter)
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
