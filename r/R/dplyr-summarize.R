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
    set_agg(
      fun = "sum",
      data = ensure_one_arg(list2(...), "sum"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding_agg("base::prod", function(..., na.rm = FALSE) {
    set_agg(
      fun = "product",
      data = ensure_one_arg(list2(...), "prod"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding_agg("base::any", function(..., na.rm = FALSE) {
    set_agg(
      fun = "any",
      data = ensure_one_arg(list2(...), "any"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding_agg("base::all", function(..., na.rm = FALSE) {
    set_agg(
      fun = "all",
      data = ensure_one_arg(list2(...), "all"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding_agg("base::mean", function(x, na.rm = FALSE) {
    set_agg(
      fun = "mean",
      data = list(x),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding_agg("stats::sd", function(x, na.rm = FALSE, ddof = 1) {
    set_agg(
      fun = "stddev",
      data = list(x),
      options = list(skip_nulls = na.rm, min_count = 0L, ddof = ddof)
    )
  })
  register_binding_agg("stats::var", function(x, na.rm = FALSE, ddof = 1) {
    set_agg(
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
      set_agg(
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
      set_agg(
        fun = "approximate_median",
        data = list(x),
        options = list(skip_nulls = na.rm)
      )
    },
    notes = "approximate median (t-digest) is computed"
  )
  register_binding_agg("dplyr::n_distinct", function(..., na.rm = FALSE) {
    set_agg(
      fun = "count_distinct",
      data = ensure_one_arg(list2(...), "n_distinct"),
      options = list(na.rm = na.rm)
    )
  })
  register_binding_agg("dplyr::n", function() {
    set_agg(
      fun = "count_all",
      data = list(),
      options = list()
    )
  })
  register_binding_agg("base::min", function(..., na.rm = FALSE) {
    set_agg(
      fun = "min",
      data = ensure_one_arg(list2(...), "min"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding_agg("base::max", function(..., na.rm = FALSE) {
    set_agg(
      fun = "max",
      data = ensure_one_arg(list2(...), "max"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
}

set_agg <- function(...) {
  agg_data <- list2(...)
  # Find the environment where ..aggregations is stored
  target <- find_aggregations_env()
  aggs <- get("..aggregations", target)
  lapply(agg_data[["data"]], function(expr) {
    # If any of the fields referenced in the expression are in ..aggregations,
    # then we can't aggregate over them.
    # This is mainly for combinations of dataset columns and aggregations,
    # like sum(x - mean(x)), i.e. window functions.
    # This will reject (sum(sum(x)) as well, but that's not a useful operation.
    if (any(expr$field_names_in_expression() %in% names(aggs))) {
      # TODO: support in ARROW-13926
      arrow_not_supported("aggregate within aggregate expression")
    }
  })

  # Record the (fun, data, options) in ..aggregations
  # and return a FieldRef pointing to it
  tmpname <- paste0("..temp", length(aggs))
  aggs[[tmpname]] <- agg_data
  assign("..aggregations", aggs, envir = target)
  Expression$field_ref(tmpname)
}

find_aggregations_env <- function() {
  # Find the environment where ..aggregations is stored,
  # it's in parent.env of something in the call stack
  for (f in sys.frames()) {
    if (exists("..aggregations", envir = f)) {
      return(f)
    }
  }
  stop("Could not find ..aggregations")
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

summarise.arrow_dplyr_query <- function(.data, ..., .by = NULL, .groups = NULL) {
  call <- match.call()
  out <- as_adq(.data)

  by <- compute_by({{ .by }}, out, by_arg = ".by", data_arg = ".data")

  if (by$from_by) {
    out$group_by_vars <- by$names
    .groups <- "drop"
  }

  exprs <- expand_across(out, quos(...), exclude_cols = out$group_by_vars)

  # Only retain the columns we need to do our aggregations
  vars_to_keep <- unique(c(
    unlist(lapply(exprs, all.vars)), # vars referenced in summarise
    dplyr::group_vars(out) # vars needed for grouping
  ))
  # If exprs rely on the results of previous exprs
  # (total = sum(x), mean = total / n())
  # then not all vars will correspond to columns in the data,
  # so don't try to select() them (use intersect() to exclude them)
  # Note that this select() isn't useful for the Arrow summarize implementation
  # because it will effectively project to keep what it needs anyway,
  # but the data.frame fallback version does benefit from select here
  out <- dplyr::select(out, intersect(vars_to_keep, names(out)))

  # Try stuff, if successful return()
  out <- try(do_arrow_summarize(out, !!!exprs, .groups = .groups), silent = TRUE)
  if (inherits(out, "try-error")) {
    out <- abandon_ship(call, .data, format(out))
  }

  out
}
summarise.Dataset <- summarise.ArrowTabular <- summarise.RecordBatchReader <- summarise.arrow_dplyr_query

# This is the Arrow summarize implementation
do_arrow_summarize <- function(.data, ..., .groups = NULL) {
  exprs <- ensure_named_exprs(quos(...))
  # Do any pre-processing to the expressions we need
  exprs <- map(
    exprs,
    adjust_summarize_expression,
    hash = length(.data$group_by_vars) > 0
  )

  # nolint start
  # summarize() is complicated because you can do a mixture of scalar operations
  # and aggregations, but that's not how Acero works. For example, for us to do
  #   summarize(mean = sum(x) / n())
  # we basically have to translate it into
  #   summarize(..temp0 = sum(x), ..temp1 = n()) %>%
  #   mutate(mean = ..temp0 / ..temp1) %>%
  #   select(-starts_with("..temp"))
  # That is, "first aggregate, then transform the result further."
  #
  # When we do filter() and mutate(), we just turn the user's code into a single
  # Arrow Expression per column. But when we do summarize(), we have to pull out
  # the aggregations, collect them in one list (that will become an Aggregate
  # ExecNode), and in the expressions, replace them with FieldRefs so that
  # further operations can happen (in what will become a ProjectNode that works
  # on the result of the Aggregate).
  # To do this, we create a list in this function scope, and in arrow_mask(),
  # and we make sure this environment here is the parent env of the binding
  # functions, so that when they receive an expression, they can pull out
  # aggregations and insert them into the list, which they can find because it
  # is in the parent env.
  # nolint end
  ..aggregations <- empty_named_list()

  # We'll collect any transformations after the aggregation here
  ..post_mutate <- empty_named_list()
  mask <- arrow_mask(.data, aggregation = TRUE)

  for (i in seq_along(exprs)) {
    # Iterate over the indices and not the names because names may be repeated
    # (which overwrites the previous name)
    name <- names(exprs)[i]
    ..post_mutate[[name]] <- summarize_eval(name, exprs[[i]], mask)
  }

  # Apply the results to the .data object.
  # First, the aggregations
  .data$aggregations <- ..aggregations
  # Then collapse the query so that the resulting query object can have
  # additional operations applied to it
  out <- collapse.arrow_dplyr_query(.data)

  # Now, add the projections in ..post_mutate (if any)
  for (post in names(..post_mutate)) {
    # One last check: it's possible that an expression like y - mean(y) would
    # successfully evaluate, but it's not supported. It gets transformed to:
    # nolint start
    #   summarize(..temp0 = mean(y)) %>%
    #   mutate(y - ..temp0)
    # nolint end
    # but y is not in the schema of the data after summarize(). To catch this
    # in the expression evaluation step, we'd have to remove all data
    # variables from the mask, which would be a bit tortured (even for me).
    # So we'll check here.
    # We can tell the expression is invalid if it references fields not in
    # the schema of the data after summarize(). Evaulating its type will
    # throw an error if it's invalid.
    tryCatch(..post_mutate[[post]]$type(out$.data$schema), error = function(e) {
      msg <- paste(
        "Expression", as_label(exprs[[post]]),
        "is not a valid aggregation expression or is"
      )
      arrow_not_supported(msg)
    })
    # If it's valid, add it to the .data object
    out$selected_columns[[post]] <- ..post_mutate[[post]]
  }

  # Make sure column order is correct (and also drop ..temp columns)
  col_order <- c(.data$group_by_vars, unique(names(exprs)))
  out$selected_columns <- out$selected_columns[col_order]

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
      ~ set_names(
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
    ~ if (hash) {
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
  map(.data$selected_columns[.data$group_by_vars], ~ .$type(schema))
}

format_aggregation <- function(x) {
  paste0(x$fun, "(", paste(map(x$data, ~ .$ToString()), collapse = ","), ")")
}

# This function evaluates an expression and returns the post-summarize
# projection that results, or NULL if there is none because the top-level
# expression was an aggregation. Any aggregations are pulled out and collected
# in the ..aggregations list outside this function.
summarize_eval <- function(name, quosure, mask) {
  # Add previous aggregations to the mask, so they can be referenced
  for (n in names(get("..aggregations", parent.frame()))) {
    mask[[n]] <- mask$.data[[n]] <- Expression$field_ref(n)
  }
  # Evaluate:
  value <- arrow_eval_or_stop(quosure, mask)

  # Handle the result. There are a few different cases.
  if (!inherits(value, "Expression")) {
    # Must have just been a scalar? (If it's not a scalar, this will error)
    # Scalars need to be added to post_mutate because they don't need
    # to be sent to the query engine as an aggregation
    value <- Expression$scalar(value)
  }

  # Handle case where outer expr is ..temp field ref. This came from an
  # aggregation at the top level. So the resulting name should be `name`.
  # not `..tempN`. Rename the corresponding aggregation.
  post_aggs <- get("..aggregations", parent.frame())
  result_field_name <- value$field_name
  if (result_field_name %in% names(post_aggs)) {
    # Do this by assigning over `name` in case something else was in `name`
    post_aggs[[name]] <- post_aggs[[result_field_name]]
    post_aggs[[result_field_name]] <- NULL
    # Assign back into the parent environment
    assign("..aggregations", post_aggs, parent.frame())
    # Return NULL because there is no post-mutate projection, it's just
    # the aggregation
    return(NULL)
  } else {
    # This is an expression that is not a ..temp fieldref, so it is some
    # function of aggregations. Return it so it can be added to post_mutate.
    return(value)
  }
}

adjust_summarize_expression <- function(quosure, hash) {
  # For the quantile() binding in the hash aggregation case, we need to mutate
  # the list output from the Arrow hash_tdigest kernel to flatten it into a
  # column of type float64. We do that by modifying the unevaluated expression
  # to replace quantile(...) with arrow_list_element(quantile(...), 0L)
  expr <- quo_get_expr(quosure)
  if (hash && any(c("quantile", "stats::quantile") %in% all_funs(expr))) {
    expr <- wrap_hash_quantile(expr)
    quo_env <- quo_get_env(quosure)
    quosure <- as_quosure(expr, quo_env)
  }
  # We could add any other adjustments here, but currently quantile is the only one
  quosure
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
