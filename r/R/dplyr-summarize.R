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

# Aggregation functions
# These all return a list of:
# @param fun string function name
# @param data list of 0 or more Expressions
# @param options list of function options, as passed to call_function
# For group-by aggregation, `hash_` gets prepended to the function name.
# So to see a list of available hash aggregation functions,
# you can use list_compute_functions("^hash_")


ensure_one_arg <- function(args, fun) {
  if (length(args) == 0) {
    arrow_not_supported(paste0(fun, "() with 0 arguments"))
  } else if (length(args) > 1) {
    arrow_not_supported(paste0("Multiple arguments to ", fun, "()"))
  }
  args
}

register_bindings_aggregate <- function() {
  register_binding_agg("base::sum", function(..., na.rm = FALSE) {
    list(
      fun = "sum",
      data = ensure_one_arg(list2(...), "sum"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding_agg("base::any", function(..., na.rm = FALSE) {
    list(
      fun = "any",
      data = ensure_one_arg(list2(...), "any"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding_agg("base::all", function(..., na.rm = FALSE) {
    list(
      fun = "all",
      data = ensure_one_arg(list2(...), "all"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding_agg("base::mean", function(x, na.rm = FALSE) {
    list(
      fun = "mean",
      data = list(x),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding_agg("stats::sd", function(x, na.rm = FALSE, ddof = 1) {
    list(
      fun = "stddev",
      data = list(x),
      options = list(skip_nulls = na.rm, min_count = 0L, ddof = ddof)
    )
  })
  register_binding_agg("stats::var", function(x, na.rm = FALSE, ddof = 1) {
    list(
      fun = "variance",
      data = list(x),
      options = list(skip_nulls = na.rm, min_count = 0L, ddof = ddof)
    )
  })
  register_binding_agg(
    "stats::quantile",
    function(x, probs, na.rm = FALSE) {
      if (length(probs) != 1) {
        arrow_not_supported("quantile() with length(probs) != 1")
      }
      # TODO: Bind to the Arrow function that returns an exact quantile and remove
      # this warning (ARROW-14021)
      warn(
        "quantile() currently returns an approximate quantile in Arrow",
        .frequency = "once",
        .frequency_id = "arrow.quantile.approximate",
        class = "arrow.quantile.approximate"
      )
      list(
        fun = "tdigest",
        data = list(x),
        options = list(skip_nulls = na.rm, q = probs)
      )
    },
    notes = c(
      "`probs` must be length 1;",
      "approximate quantile (t-digest) is computed"
    )
  )
  register_binding_agg(
    "stats::median",
    function(x, na.rm = FALSE) {
      # TODO: Bind to the Arrow function that returns an exact median and remove
      # this warning (ARROW-14021)
      warn(
        "median() currently returns an approximate median in Arrow",
        .frequency = "once",
        .frequency_id = "arrow.median.approximate",
        class = "arrow.median.approximate"
      )
      list(
        fun = "approximate_median",
        data = list(x),
        options = list(skip_nulls = na.rm)
      )
    },
    notes = "approximate median (t-digest) is computed"
  )
  register_binding_agg("dplyr::n_distinct", function(..., na.rm = FALSE) {
    list(
      fun = "count_distinct",
      data = ensure_one_arg(list2(...), "n_distinct"),
      options = list(na.rm = na.rm)
    )
  })
  register_binding_agg("dplyr::n", function() {
    list(
      fun = "count_all",
      data = list(),
      options = list()
    )
  })
  register_binding_agg("base::min", function(..., na.rm = FALSE) {
    list(
      fun = "min",
      data = ensure_one_arg(list2(...), "min"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding_agg("base::max", function(..., na.rm = FALSE) {
    list(
      fun = "max",
      data = ensure_one_arg(list2(...), "max"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
}

# we register 2 versions of the "::" binding - one for use with agg_funcs
# (registered below) and another one for use with nse_funcs
# (registered in dplyr-funcs.R)
agg_funcs[["::"]] <- function(lhs, rhs) {
  lhs_name <- as.character(substitute(lhs))
  rhs_name <- as.character(substitute(rhs))

  fun_name <- paste0(lhs_name, "::", rhs_name)

  # if we do not have a binding for pkg::fun, then fall back on to the
  # nse_funcs (useful when we have a regular function inside an aggregating one)
  # and then, if searching nse_funcs fails too, fall back to the
  # regular `pkg::fun()` function
  agg_funcs[[fun_name]] %||% nse_funcs[[fun_name]] %||% asNamespace(lhs_name)[[rhs_name]]
}

# The following S3 methods are registered on load if dplyr is present

summarise.arrow_dplyr_query <- function(.data, ..., .groups = NULL) {
  call <- match.call()
  .data <- as_adq(.data)
  exprs <- expand_across(.data, quos(...), exclude_cols = .data$group_by_vars)
  # Only retain the columns we need to do our aggregations
  vars_to_keep <- unique(c(
    unlist(lapply(exprs, all.vars)), # vars referenced in summarise
    dplyr::group_vars(.data) # vars needed for grouping
  ))
  # If exprs rely on the results of previous exprs
  # (total = sum(x), mean = total / n())
  # then not all vars will correspond to columns in the data,
  # so don't try to select() them (use intersect() to exclude them)
  # Note that this select() isn't useful for the Arrow summarize implementation
  # because it will effectively project to keep what it needs anyway,
  # but the data.frame fallback version does benefit from select here
  .data <- dplyr::select(.data, intersect(vars_to_keep, names(.data)))

  # Try stuff, if successful return()
  out <- try(do_arrow_summarize(.data, !!!exprs, .groups = .groups), silent = TRUE)
  if (inherits(out, "try-error")) {
    return(abandon_ship(call, .data, format(out)))
  } else {
    return(out)
  }
}
summarise.Dataset <- summarise.ArrowTabular <- summarise.RecordBatchReader <- summarise.arrow_dplyr_query

# This is the Arrow summarize implementation
do_arrow_summarize <- function(.data, ..., .groups = NULL) {
  exprs <- ensure_named_exprs(quos(...))

  # Create a stateful environment for recording our evaluated expressions
  # It's more complex than other places because a single summarize() expr
  # may result in multiple query nodes (Aggregate, Project),
  # and we have to walk through the expressions to disentangle them.
  ctx <- env(
    mask = arrow_mask(.data, aggregation = TRUE),
    aggregations = empty_named_list(),
    post_mutate = empty_named_list()
  )
  for (i in seq_along(exprs)) {
    # Iterate over the indices and not the names because names may be repeated
    # (which overwrites the previous name)
    summarize_eval(
      names(exprs)[i],
      exprs[[i]],
      ctx,
      length(.data$group_by_vars) > 0
    )
  }

  # Apply the results to the .data object.
  # First, the aggregations
  .data$aggregations <- ctx$aggregations
  # Then collapse the query so that the resulting query object can have
  # additional operations applied to it
  out <- collapse.arrow_dplyr_query(.data)
  # The expressions may have been translated into
  # "first, aggregate, then transform the result further"
  # nolint start
  # For example,
  #   summarize(mean = sum(x) / n())
  # is effectively implemented as
  #   summarize(..temp0 = sum(x), ..temp1 = n()) %>%
  #   mutate(mean = ..temp0 / ..temp1) %>%
  #   select(-starts_with("..temp"))
  # If this is the case, there will be expressions in post_mutate
  # nolint end
  if (length(ctx$post_mutate)) {
    # Append post_mutate, and make sure order is correct
    # according to input exprs (also dropping ..temp columns)
    out$selected_columns <- c(
      out$selected_columns,
      ctx$post_mutate
    )[c(.data$group_by_vars, names(exprs))]
  }

  # If the object has .drop = FALSE and any group vars are dictionaries,
  # we can't (currently) preserve the empty rows that dplyr does,
  # so give a warning about that.
  if (!dplyr::group_by_drop_default(.data)) {
    group_by_exprs <- .data$selected_columns[.data$group_by_vars]
    if (any(map_lgl(group_by_exprs, ~ inherits(.$type(), "DictionaryType")))) {
      warning(
        ".drop = FALSE currently not supported in Arrow aggregation",
        call. = FALSE
      )
    }
  }

  # Handle .groups argument
  if (length(.data$group_by_vars)) {
    if (is.null(.groups)) {
      # dplyr docs say:
      # When ‘.groups’ is not specified, it is chosen based on the
      # number of rows of the results:
      # • If all the results have 1 row, you get "drop_last".
      # • If the number of rows varies, you get "keep".
      #
      # But we don't support anything that returns multiple rows now
      .groups <- "drop_last"
    } else {
      assert_that(is.string(.groups))
    }
    if (.groups == "drop_last") {
      out$group_by_vars <- head(.data$group_by_vars, -1)
    } else if (.groups == "keep") {
      out$group_by_vars <- .data$group_by_vars
    } else if (.groups == "rowwise") {
      stop(arrow_not_supported('.groups = "rowwise"'))
    } else if (.groups == "drop") {
      # collapse() preserves groups so remove them
      out <- dplyr::ungroup(out)
    } else {
      stop(paste("Invalid .groups argument:", .groups))
    }
    out$drop_empty_groups <- .data$drop_empty_groups
    if (getOption("arrow.summarise.sort", FALSE)) {
      # Add sorting instructions for the rows to match dplyr
      out$arrange_vars <- .data$selected_columns[.data$group_by_vars]
      out$arrange_desc <- rep(FALSE, length(.data$group_by_vars))
    }
  }
  out
}

arrow_eval_or_stop <- function(expr, mask) {
  # TODO: change arrow_eval error handling behavior?
  out <- arrow_eval(expr, mask)
  if (inherits(out, "try-error")) {
    msg <- handle_arrow_not_supported(out, format_expr(expr))
    stop(msg, call. = FALSE)
  }
  out
}

# This function returns a list of expressions which is used to project the data
# before an aggregation. This list includes the fields used in the aggregation
# expressions (the "targets") and the group fields. The names of the returned
# list are used to ensure that the projection node is wired up correctly to the
# aggregation node.
summarize_projection <- function(.data) {
  c(
    unlist(unname(imap(
      .data$aggregations,
      ~set_names(
        .x$data,
        aggregate_target_names(.x$data, .y)
      )
    ))),
    .data$selected_columns[.data$group_by_vars]
  )
}

# This function determines what names to give to the fields used in an
# aggregation expression (the "targets"). When an aggregate function takes 2 or
# more fields as targets, this function gives the fields unique names by
# appending `..1`, `..2`, etc. When an aggregate function is nullary, this
# function returns a zero-length character vector.
aggregate_target_names <- function(data, name) {
  if (length(data) > 1) {
    paste(name, seq_along(data), sep = "..")
  } else if (length(data) > 0) {
    name
  } else {
    character(0)
  }
}

# This function returns a named list of the data types of the aggregate columns
# returned by an aggregation
aggregate_types <- function(.data, hash, schema = NULL) {
  if (hash) dummy_groups <- Scalar$create(1L, uint32())
  map(
    .data$aggregations,
    ~if (hash) {
      Expression$create(
        paste0("hash_", .$fun),
        # hash aggregate kernels must be passed an additional argument
        # representing the groups, so we pass in a dummy scalar, since the
        # groups will not affect the type that an aggregation returns
        args = c(.$data, dummy_groups),
        options = .$options
      )$type(schema)
    } else {
      Expression$create(
        .$fun,
        args = .$data,
        options = .$options
      )$type(schema)
    }
  )
}

# This function returns a named list of the data types of the group columns
# returned by an aggregation
group_types <- function(.data, schema = NULL) {
  map(.data$selected_columns[.data$group_by_vars], ~.$type(schema))
}

format_aggregation <- function(x) {
  paste0(x$fun, "(", paste(map(x$data, ~.$ToString()), collapse = ","), ")")
}

# This function handles each summarize expression and turns it into the
# appropriate combination of (1) aggregations (possibly temporary) and
# (2) post-aggregation transformations (mutate)
# The function returns nothing: it assigns into the `ctx` environment
summarize_eval <- function(name, quosure, ctx, hash) {
  expr <- quo_get_expr(quosure)
  ctx$quo_env <- quo_get_env(quosure)

  funs_in_expr <- all_funs(expr)

  if (length(funs_in_expr) == 0) {
    # This branch only gets called at the top level, where expr is something
    # that is not a function call (could be a quosure, a symbol, or atomic
    # value). This needs to evaluate to a scalar or something that can be
    # converted to one.
    value <- arrow_eval_or_stop(quosure, ctx$mask)

    if (!inherits(value, "Expression")) {
      value <- Expression$scalar(value)
    }

    # We can't support a bare field reference because this is not
    # an aggregate expression
    if (!identical(value$field_name, "")) {
      abort(
        paste(
          "Expression", format_expr(quosure),
          "is not an aggregate expression or is not supported in Arrow"
        )
      )
    }

    # Scalars need to be added to post_mutate because they don't need
    # to be sent to the query engine as an aggregation
    ctx$post_mutate[[name]] <- value
    return()
  }

  # For the quantile() binding in the hash aggregation case, we need to mutate
  # the list output from the Arrow hash_tdigest kernel to flatten it into a
  # column of type float64. We do that by modifying the unevaluated expression
  # to replace quantile(...) with arrow_list_element(quantile(...), 0L)
  if (hash && any(c("quantile", "stats::quantile") %in% funs_in_expr)) {
    expr <- wrap_hash_quantile(expr)
    funs_in_expr <- all_funs(expr)
  }

  # Start inspecting the expr to see what aggregations it involves
  agg_funs <- names(agg_funcs)
  outer_agg <- funs_in_expr[1] %in% agg_funs
  inner_agg <- funs_in_expr[-1] %in% agg_funs

  # First, pull out any aggregations wrapped in other function calls
  if (any(inner_agg)) {
    expr <- extract_aggregations(expr, ctx)
  }

  # By this point, there are no more aggregation functions in expr
  # except for possibly the outer function call:
  # they've all been pulled out to ctx$aggregations, and in their place in expr
  # there are variable names, which would correspond to field refs in the
  # query object after aggregation and collapse() or non-field variable
  # references. So if we want to know if there are any aggregations inside expr,
  # we have to look for them by their new var names in ctx$aggregations.
  inner_agg_exprs <- all_vars(expr) %in% names(ctx$aggregations)
  inner_is_fieldref <- all_vars(expr) %in% names(ctx$mask$.data)

  if (outer_agg) {
    # This is something like agg(fun(x, y)
    # It just works by normal arrow_eval, unless there's a mix of aggs and
    # columns in the original data like agg(fun(x, agg(x)))
    # (but that will have been caught in extract_aggregations())
    ctx$aggregations[[name]] <- arrow_eval_or_stop(
      as_quosure(expr, ctx$quo_env),
      ctx$mask
    )
    return()
  } else if (all(inner_agg_exprs | !inner_is_fieldref)) {
    # Something like: fun(agg(x), agg(y))
    # So based on the aggregations that have been extracted, mutate after
    agg_field_refs <- make_field_refs(names(ctx$aggregations))
    agg_field_types <- aggregate_types(ctx$aggregations, hash)

    mutate_mask <- arrow_mask(
      list(
        selected_columns = agg_field_refs,
        .data = list(
          schema = schema(!!!agg_field_types)
        )
      )
    )

    value <- arrow_eval_or_stop(
      as_quosure(expr, ctx$quo_env),
      mutate_mask
    )

    if (!inherits(value, "Expression")) {
      value <- Expression$scalar(value)
    }

    ctx$post_mutate[[name]] <- value
    return()
  }

  # Backstop for any other odd cases, like fun(x, y) (i.e. no aggregation),
  # or aggregation functions that aren't supported in Arrow (not in agg_funcs)
  abort(
    paste(
      "Expression", format_expr(quosure),
      "is not an aggregate expression or is not supported in Arrow"
    )
  )
}

# This function recurses through expr, pulls out any aggregation expressions,
# and inserts a variable name (field ref) in place of the aggregation
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
    inner_agg_exprs <- all_vars(expr) %in% names(ctx$aggregations)
    if (any(inner_agg_exprs)) {
      # We can't aggregate over a combination of dataset columns and other
      # aggregations (e.g. sum(x - mean(x)))
      # TODO: support in ARROW-13926
      abort(
        paste(
          "Aggregate within aggregate expression",
          format_expr(original_expr),
          "not supported in Arrow"
        )
      )
    }

    # We have an aggregation expression with no other aggregations inside it,
    # so arrow_eval the expression on the data and give it a ..temp name prefix,
    # then insert that name (symbol) back into the expression so that we can
    # mutate() on the result of the aggregation and reference this field.
    tmpname <- paste0("..temp", length(ctx$aggregations))
    ctx$aggregations[[tmpname]] <- arrow_eval_or_stop(as_quosure(expr, ctx$quo_env), ctx$mask)
    expr <- as.symbol(tmpname)
  }
  expr
}

# This function recurses through expr and wraps each call to quantile() with a
# call to arrow_list_element()
wrap_hash_quantile <- function(expr) {
  if (length(expr) == 1) {
    return(expr)
  } else {
    if (is.call(expr) && any(c(quote(quantile), quote(stats::quantile)) == expr[[1]])) {
      return(str2lang(paste0("arrow_list_element(", deparse1(expr), ", 0L)")))
    } else {
      return(as.call(lapply(expr, wrap_hash_quantile)))
    }
  }
}
