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
  .data <- arrow_dplyr_query(.data)
  exprs <- quos(...)
  # Only retain the columns we need to do our aggregations
  vars_to_keep <- unique(c(
    unlist(lapply(exprs, all.vars)), # vars referenced in summarise
    dplyr::group_vars(.data) # vars needed for grouping
  ))
  .data <- dplyr::select(.data, vars_to_keep)
  if (match.arg(.engine) == "duckdb") {
      dplyr::summarise(to_duckdb(.data), ...)
  } else if (isTRUE(getOption("arrow.summarize", FALSE))) {
    # Try stuff, if successful return()
    out <- try(do_arrow_summarize(.data, ...), silent = TRUE)
    if (inherits(out, "try-error")) {
      return(abandon_ship(call, .data, format(out)))
    } else {
      return(out)
    }
  } else {
    # If unsuccessful or if option not set, do the work in R
    dplyr::summarise(dplyr::collect(.data), ...)
  }
}
summarise.Dataset <- summarise.ArrowTabular <- summarise.arrow_dplyr_query

do_arrow_summarize <- function(.data, ...) {
  exprs <- quos(...)
  # Check for unnamed expressions and fix if any
  unnamed <- !nzchar(names(exprs))
  # Deparse and take the first element in case they're long expressions
  names(exprs)[unnamed] <- map_chr(exprs[unnamed], as_label)

  mask <- arrow_mask(.data)
  # Add aggregation wrappers to arrow_mask somehow
  # (this is not ideal, would overwrite same-named objects)
  mask$sum <- function(x, na.rm = FALSE) {
    list(
      fun = "sum",
      data = x,
      options = list(na.rm = na.rm, na.min_count = 0L)
    )
  }
  results <- list()
  for (i in seq_along(exprs)) {
    # Iterate over the indices and not the names because names may be repeated
    # (which overwrites the previous name)
    new_var <- names(exprs)[i]
    results[[new_var]] <- arrow_eval(exprs[[i]], mask)
    if (inherits(results[[new_var]], "try-error")) {
      msg <- paste('Expression', as_label(exprs[[i]]), 'not supported in Arrow')
      stop(msg, call. = FALSE)
    }
    # Put it in the data mask too?
    #mask[[new_var]] <- mask$.data[[new_var]] <- results[[new_var]]
  }

  # Now, from that, split out the data (expressions) and options
  .data$aggregations <- lapply(results, function(x) x[c("fun", "options")])

  inputs <- lapply(results, function(x) x$data)
  # This is essentially a projection, and the column names don't matter
  # (but must exist)
  names(inputs) <- as.character(seq_along(inputs))
  .data$selected_columns <- inputs

  # Eventually, we will return .data here if (dataset) but do it eagerly now
  do_exec_plan(.data, group_vars = dplyr::group_vars(.data))
}

do_exec_plan <- function(.data, group_vars = NULL) {
  plan <- ExecPlan$create()

  if (length(group_vars) == 0) {
    # Scan also will filter and select columns, so we don't need to Filter
    start_node <- plan$Scan(.data)
    # If any columns are derived we need to Project (otherwise this may be no-op)
    project_node <- start_node$Project(.data$selected_columns)
    final_node <- project_node$ScalarAggregate(
      options = .data$aggregations,
      target_names = names(.data),
      out_field_names = names(.data$aggregations)
    )
  } else {
    # Collect the target names first because we have to add back the group vars
    target_names <- names(.data)
    .data <- ensure_group_vars(.data)

    # We also need to prefix all of the aggregation function names with "hash_"
    .data$aggregations <- lapply(.data$aggregations, function(x) {
      x[["fun"]] <- paste0("hash_", x[["fun"]])
      x
    })
    # Scan also will filter and select columns, so we don't need to Filter
    start_node <- plan$Scan(.data)
    # If any columns are derived we need to Project (otherwise this may be no-op)
    project_node <- start_node$Project(.data$selected_columns)
    final_node <- project_node$GroupByAggregate(
      group_vars,
      target_names = target_names,
      aggregations = .data$aggregations
    )
  }
  plan$Run(final_node)
}