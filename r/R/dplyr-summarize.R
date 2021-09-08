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
  .data <- dplyr::select(.data, intersect(vars_to_keep, names(.data)))
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

  # Create a stateful environment for recording our evaluated expressions
  # It's more complex than other places because a single summarize() expr
  # may result in multiple query nodes (Aggregate, Project)
  ctx <- env(
    mask = arrow_mask(.data, aggregation = TRUE),
    results = empty_named_list(),
    post_mutate = empty_named_list()
  )
  for (i in seq_along(exprs)) {
    # Iterate over the indices and not the names because names may be repeated
    # (which overwrites the previous name)
    summarize_eval(names(exprs)[i], exprs[[i]], ctx)
  }

  .data$aggregations <- ctx$results
  out <- collapse.arrow_dplyr_query(.data)
  if (length(ctx$post_mutate)) {
    # mutate()
    # TODO: get order of columns correct
    out$selected_columns <- c(out$selected_columns[-grep("^\\.\\.temp", names(out$selected_columns))], ctx$post_mutate)
  }
  out
}

arrow_eval_or_stop <- function(expr, mask) {
  # TODO: change arrow_eval error handling behavior?
  out <- arrow_eval(expr, mask)
  if (inherits(out, "try-error")) {
    msg <- handle_arrow_not_supported(out, as_label(expr))
    stop(msg, call. = FALSE)
  }
  out
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

# Cases:
# * agg(fun(x, y)): OK
# * fun(agg(x), agg(y)): TODO now: pull out aggregates, insert fieldref, then mutate
# * z = agg(x); fun(z, agg(y)): TODO now
# * agg(fun(agg(x), agg(y))): TODO now too? is this meaningful? (dplyr doesn't error on it)
# * fun(agg(x), y): Later (implicit join; seems to be equivalent to doing it in mutate)
# * z = agg(x); fun(z, y): Later (same, implicit join)

# find aggregation subcomponents
# eval, insert fieldref; give "..temp" prefix to name
# record fieldrefs in list and in mask
#

summarize_eval <- function(name, quosure, ctx, recurse = FALSE) {
  expr <- quo_get_expr(quosure)
  ctx$quo_env <- quo_get_env(quosure)
  funs_in_expr <- all_funs(expr)

  if (length(funs_in_expr) == 0) {
    # Skip if it is a scalar or field ref
    ctx$results[[name]] <- arrow_eval_or_stop(quosure, ctx$mask)
    return()
  }

  agg_funs <- names(agg_funcs)
  outer_agg <- funs_in_expr[1] %in% agg_funs
  inner_agg <- funs_in_expr[-1] %in% agg_funs

  # First, pull out any aggregations wrapped in other function calls
  if (any(inner_agg)) {
    expr <- extract_aggregations(expr, ctx)
  }

  inner_agg_exprs <- all_vars(expr) %in% names(ctx$results)

  if (outer_agg) {
    # This just works by normal arrow_eval, unless there's a mix of aggs and
    # columns in the original data like agg(fun(x, agg(x)))
    # (but that will have been caught in extract_aggregations())
    ctx$results[[name]] <- arrow_eval_or_stop(quosure, ctx$mask)
    return()
  } else if (all(inner_agg_exprs)) {
    # fun(agg(x), ...)
    # So based on the aggregations that have been extracted, mutate after
    mutate_mask <- arrow_mask(list(selected_columns = make_field_refs(names(ctx$results))))
    ctx$post_mutate[[name]] <- arrow_eval_or_stop(as_quosure(expr, ctx$quo_env), mutate_mask)
    return()
  }
  # !outer_agg && !all(inner_agg_exprs)
  # This is fun(x, agg(y)), which really should be in mutate()
  # but summarize() allows it. (See also below in extract_aggregations)
  # TODO: support in ARROW-13926
  # (This could also be fun(x, y), which would work in mutate() already
  # if it were the only expression)
  # TODO: this message should probably also say "not supported in summarize()"
  # since some of these expressions may be legal elsewhere
  stop(handle_arrow_not_supported(quo_get_expr(quosure), as_label(quo_get_expr(quosure))), call. = FALSE)
}

extract_aggregations <- function(expr, ctx) {
  # Keep the input in case we need to raise an error message with it
  original_expr <- expr
  funs <- all_funs(expr)
  if (length(funs) == 0) {
    return(expr)
  } else if (length(funs) > 1) {
    # Recurse more
    expr[-1] <- lapply(expr[-1], extract_aggregations, ctx)
  }
  if (funs[1] %in% names(agg_funcs)) {
    inner_agg_exprs <- all_vars(expr) %in% names(ctx$results)
    if (any(inner_agg_exprs) & !all(inner_agg_exprs)) {
      # We can't aggregate over a combination of dataset columns and other
      # aggregations (e.g. sum(x - mean(x)))
      # TODO: Add "because" arg to explain _why_ it's not supported?
      # TODO: support in ARROW-13926
      stop(handle_arrow_not_supported(original_expr, as_label(original_expr)), call. = FALSE)
    }

    tmpname <- paste0("..temp", length(ctx$results))
    ctx$results[[tmpname]] <- arrow_eval_or_stop(as_quosure(expr, ctx$quo_env), ctx$mask)
    expr <- as.symbol(tmpname)
  }
  expr
}
