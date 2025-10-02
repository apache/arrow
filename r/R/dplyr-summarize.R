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

summarise.arrow_dplyr_query <- function(.data, ..., .by = NULL, .groups = NULL) {
  try_arrow_dplyr({
    out <- as_adq(.data)

    by <- compute_by({{ .by }}, out, by_arg = ".by", data_arg = ".data")
    if (by$from_by) {
      out$group_by_vars <- by$names
      .groups <- "drop"
    }

    exprs <- expand_across(out, quos(...), exclude_cols = out$group_by_vars)
    do_arrow_summarize(out, !!!exprs, .groups = .groups)
  })
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

  # Do a projection here to keep only the columns we need in summarize().
  # If possible, this will push down the column selection into the SourceNode,
  # saving lots of wasted processing for columns we don't need. (GH-43627)
  vars_to_keep <- unique(c(
    unlist(lapply(exprs, all.vars)), # vars referenced in summarize
    dplyr::group_vars(.data) # vars needed for grouping
  ))
  .data <- dplyr::select(.data, intersect(vars_to_keep, names(.data)))

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
  # To do this, arrow_mask() includes a list called .aggregations,
  # and the aggregation functions will pull out those terms and insert into
  # that list.
  # nolint end
  mask <- arrow_mask(.data)

  # We'll collect any transformations after the aggregation here.
  # summarize_eval() returns NULL when the outer expression is an aggregation,
  # i.e. there is no projection to do after
  post_mutate <- empty_named_list()
  for (i in seq_along(exprs)) {
    # Iterate over the indices and not the names because names may be repeated
    # (which overwrites the previous name)
    name <- names(exprs)[i]
    post_mutate[[name]] <- summarize_eval(name, exprs[[i]], mask)
  }

  # Apply the results to the .data object.
  # First, the aggregations
  .data$aggregations <- mask$.aggregations
  # Then collapse the query so that the resulting query object can have
  # additional operations applied to it
  out <- collapse.arrow_dplyr_query(.data)

  # Now, add the projections in post_mutate (if any)
  for (post in names(post_mutate)) {
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
    # the schema of the data after summarize(). Evaluating its type will
    # throw an error if it's invalid.
    tryCatch(post_mutate[[post]]$type(out$.data$schema), error = function(e) {
      arrow_not_supported(
        "Expression is not a valid aggregation expression or is",
        call = exprs[[post]]
      )
    })
    # If it's valid, add it to the .data object
    out$selected_columns[[post]] <- post_mutate[[post]]
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
      arrow_not_supported(
        '.groups = "rowwise"',
        call = rlang::caller_call()
      )
    } else if (.groups == "drop") {
      # collapse() preserves groups so remove them
      out <- dplyr::ungroup(out)
    } else {
      validation_error(
        paste("Invalid .groups argument:", .groups),
        call = rlang::caller_call()
      )
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
  Expression$create(x$fun, args = x$data, options = x$options)$ToString()
}

# This function evaluates an expression and returns the post-summarize
# projection that results, or NULL if there is none because the top-level
# expression was an aggregation. Any aggregations are pulled out and collected
# in the .aggregations list outside this function.
summarize_eval <- function(name, quosure, mask) {
  # Add previous aggregations to the mask, so they can be referenced
  for (n in names(mask$.aggregations)) {
    mask[[n]] <- mask$.data[[n]] <- Expression$field_ref(n)
  }
  # Evaluate:
  value <- arrow_eval(quosure, mask)

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
  result_field_name <- value$field_name
  if (result_field_name %in% names(mask$.aggregations)) {
    # Do this by assigning over `name` in case something else was in `name`
    mask$.aggregations[[name]] <- mask$.aggregations[[result_field_name]]
    mask$.aggregations[[result_field_name]] <- NULL
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
