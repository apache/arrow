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

summarise.arrow_dplyr_query <- function(.data, ..., .engine = c("arrow", "duckdb")) {
  call <- match.call()
  .data <- as_adq(.data)
  exprs <- quos(...)
  # Only retain the columns we need to do our aggregations
  vars_to_keep <- unique(c(
    unlist(lapply(exprs, all.vars)), # vars referenced in summarise
    dplyr::group_vars(.data) # vars needed for grouping
  ))
  .data <- dplyr::select(.data, vars_to_keep)
  if (match.arg(.engine) == "duckdb") {
    dplyr::summarise(to_duckdb(.data), ...)
  } else {
    # Try stuff, if successful return()
    out <- try(do_arrow_summarize(.data, ...), silent = TRUE)
    if (inherits(out, "try-error")) {
      return(abandon_ship(call, .data, format(out)))
    } else {
      return(out)
    }
  }
}
summarise.Dataset <- summarise.ArrowTabular <- summarise.arrow_dplyr_query

do_arrow_summarize <- function(.data, ..., .groups = NULL) {
  if (!is.null(.groups)) {
    # ARROW-13550
    abort("`summarize()` with `.groups` argument not supported in Arrow")
  }
  exprs <- ensure_named_exprs(quos(...))

  mask <- arrow_mask(.data, aggregation = TRUE)

  results <- list()
  for (i in seq_along(exprs)) {
    # Iterate over the indices and not the names because names may be repeated
    # (which overwrites the previous name)
    new_var <- names(exprs)[i]
    results[[new_var]] <- arrow_eval(exprs[[i]], mask)
    if (inherits(results[[new_var]], "try-error")) {
      msg <- handle_arrow_not_supported(
        results[[new_var]],
        as_label(exprs[[i]])
      )
      stop(msg, call. = FALSE)
    }
  }

  .data$aggregations <- results
  # TODO: should in-memory query evaluate eagerly?
  collapse.arrow_dplyr_query(.data)
}

summarize_projection <- function(.data) {
  c(
    map(.data$aggregations, ~ .$data),
    .data$selected_columns[.data$group_by_vars]
  )
}

format_aggregation <- function(x) {
  paste0(x$fun, "(", x$data$ToString(), ")")
}
