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

mutate.arrow_dplyr_query <- function(.data,
                                     ...,
                                     .keep = c("all", "used", "unused", "none"),
                                     .before = NULL,
                                     .after = NULL) {
  call <- match.call()

  expression_list <- expand_across(.data, quos(...))
  exprs <- ensure_named_exprs(expression_list)

  .keep <- match.arg(.keep)
  .before <- enquo(.before)
  .after <- enquo(.after)

  if (.keep %in% c("all", "unused") && length(exprs) == 0) {
    # Nothing to do
    return(.data)
  }

  .data <- as_adq(.data)

  # Restrict the cases we support for now
  has_aggregations <- any(unlist(lapply(exprs, all_funs)) %in% names(agg_funcs))
  if (has_aggregations) {
    # ARROW-13926
    # mutate() on a grouped dataset does calculations within groups
    # This doesn't matter on scalar ops (arithmetic etc.) but it does
    # for things with aggregations (e.g. subtracting the mean)
    return(abandon_ship(call, .data, "window functions not currently supported in Arrow"))
  }

  mask <- arrow_mask(.data)
  results <- list()
  for (i in seq_along(exprs)) {
    # Iterate over the indices and not the names because names may be repeated
    # (which overwrites the previous name)
    new_var <- names(exprs)[i]
    results[[new_var]] <- arrow_eval(exprs[[i]], mask)
    if (inherits(results[[new_var]], "try-error")) {
      msg <- handle_arrow_not_supported(
        results[[new_var]],
        format_expr(exprs[[i]])
      )
      return(abandon_ship(call, .data, msg))
    } else if (!inherits(results[[new_var]], "Expression") &&
      !is.null(results[[new_var]])) {
      # We need some wrapping to handle literal values
      if (length(results[[new_var]]) != 1) {
        msg <- paste0("In ", new_var, " = ", format_expr(exprs[[i]]), ", only values of size one are recycled")
        return(abandon_ship(call, .data, msg))
      }
      results[[new_var]] <- Expression$scalar(results[[new_var]])
    }
    # Put it in the data mask too
    mask[[new_var]] <- mask$.data[[new_var]] <- results[[new_var]]
  }

  old_vars <- names(.data$selected_columns)
  # Note that this is names(exprs) not names(results):
  # if results$new_var is NULL, that means we are supposed to remove it
  new_vars <- names(exprs)

  # Assign the new columns into the .data$selected_columns
  for (new_var in new_vars) {
    .data$selected_columns[[new_var]] <- results[[new_var]]
  }

  # Deduplicate new_vars and remove NULL columns from new_vars
  new_vars <- intersect(new_vars, names(.data$selected_columns))

  # Respect .before and .after
  if (!quo_is_null(.before) || !quo_is_null(.after)) {
    new <- setdiff(new_vars, old_vars)
    .data <- dplyr::relocate(.data, all_of(new), .before = !!.before, .after = !!.after)
  }

  # Respect .keep
  if (.keep == "none") {
    ## for consistency with dplyr, this appends new columns after existing columns
    ## by specifying the order
    new_cols_last <- c(intersect(old_vars, new_vars), setdiff(new_vars, old_vars))
    .data$selected_columns <- .data$selected_columns[new_cols_last]
  } else if (.keep != "all") {
    # "used" or "unused"
    used_vars <- unlist(lapply(exprs, all.vars), use.names = FALSE)
    if (.keep == "used") {
      .data$selected_columns[setdiff(old_vars, used_vars)] <- NULL
    } else {
      # "unused"
      .data$selected_columns[intersect(old_vars, used_vars)] <- NULL
    }
  }
  # Even if "none", we still keep group vars
  ensure_group_vars(.data)
}
mutate.Dataset <- mutate.ArrowTabular <- mutate.RecordBatchReader <- mutate.arrow_dplyr_query

transmute.arrow_dplyr_query <- function(.data, ...) {
  dots <- check_transmute_args(...)
  has_null <- map_lgl(dots, quo_is_null)
  .data <- dplyr::mutate(.data, !!!dots, .keep = "none")
  if (is_empty(dots) || any(has_null)) {
    return(.data)
  }

  ## keeping with: https://github.com/tidyverse/dplyr/issues/6086
  cur_exprs <- map_chr(dots, as_label)
  transmute_order <- names(cur_exprs)
  transmute_order[!nzchar(transmute_order)] <- cur_exprs[!nzchar(transmute_order)]
  dplyr::select(.data, all_of(transmute_order))
}
transmute.Dataset <- transmute.ArrowTabular <- transmute.RecordBatchReader <- transmute.arrow_dplyr_query

# This function is a copy of dplyr:::check_transmute_args at
# https://github.com/tidyverse/dplyr/blob/master/R/mutate.R
check_transmute_args <- function(..., .keep, .before, .after) {
  if (!missing(.keep)) {
    abort("`transmute()` does not support the `.keep` argument")
  }
  if (!missing(.before)) {
    abort("`transmute()` does not support the `.before` argument")
  }
  if (!missing(.after)) {
    abort("`transmute()` does not support the `.after` argument")
  }
  enquos(...)
}

ensure_named_exprs <- function(exprs) {
  # Check for unnamed expressions and fix if any
  unnamed <- !nzchar(names(exprs))
  # Deparse and take the first element in case they're long expressions
  names(exprs)[unnamed] <- map_chr(exprs[unnamed], format_expr)
  exprs
}

# Take the input quos and unfold any instances of across()
# into individual quosures
expand_across <- function(.data, quos_in) {
  quos_out <- list()
  # Check for any expressions starting with across
  for (quo_i in seq_along(quos_in)) {
    # do it like this to preserve naming
    quo_in <- quos_in[quo_i]
    quo_expr <- quo_get_expr(quo_in[[1]])
    quo_env <- quo_get_env(quo_in[[1]])

    if (is_call(quo_expr, "across")) {
      new_quos <- list()

      across_call <- match.call(
        definition = dplyr::across,
        call = quo_expr,
        expand.dots = FALSE,
        envir = quo_env
      )

      if (!all(names(across_call[-1]) %in% c(".cols", ".fns", ".names"))) {
        abort("`...` argument to `across()` is deprecated in dplyr and not supported in Arrow")
      }

      if (!is.null(across_call[[".cols"]])) {
        cols <- across_call[[".cols"]]
      } else {
        cols <- quote(everything())
      }

      cols <- as_quosure(cols, quo_env)

      setup <- across_setup(
        !!cols,
        fns = across_call[[".fns"]],
        names = across_call[[".names"]],
        .caller_env = quo_env,
        mask = .data,
        inline = TRUE
      )

      # calling across() with .fns = NULL returns all columns unchanged
      if (is_empty(setup$fns)) {
        # this needs to be updated to match dplyr's version
        return()
      }

      if (!is_list(setup$fns) && as.character(setup$fns)[[1]] == "~") {
        abort(
          paste(
            "purrr-style lambda functions as `.fns` argument to `across()`",
            "not yet supported in Arrow"
          )
        )
      }

      # if only 1 function, we overwrite the old columns with the new values
      if (length(setup$fns) == 0 && is.name(setup$fns)) {
        # work out the quosures from the call
        col_syms <- syms(setup$vars)
        new_quos <- map(col_syms, ~ quo(!!call2(setup$fns, .x)))
        new_quos <- set_names(new_quos, setup$names)
      } else {
        new_quos <- quosures_from_func_list(setup, quo_env)
      }

      quos_out <- append(quos_out, new_quos)
    } else {
      quos_out <- append(quos_out, quo_in)
    }
  }

  quos_out
}

# given a named list of functions and column names, create a list of new quosures
quosures_from_func_list <- function(setup, quo_env) {
  func_list_full <- rep(setup$fns, length(setup$vars))
  cols_list_full <- rep(setup$vars, each = length(setup$fns))

  # get new quosures
  new_quo_list <- map2(
    func_list_full, cols_list_full,
    ~ quo(!!call2(.x, sym(.y)))
  )

  quosures <- set_names(new_quo_list, setup$names)
  map(quosures, ~quo_set_env(.x, quo_env))
}

across_setup <- function(cols, fns, names, .caller_env, mask, inline = FALSE){
  cols <- enquo(cols)

  if (is.null(fns) && quo_is_call(cols, "~")) {
    bullets <- c(
      "Must supply a column selection.",
      i = glue("You most likely meant: `{across_if_fn}(everything(), {as_label(cols)})`."),
      i = "The first argument `.cols` selects a set of columns.",
      i = "The second argument `.fns` operates on each selected columns."
    )
    abort(bullets, call = call(across_if_fn))
  }
  vars <- names(dplyr::select(mask, !!cols))

  # need to work out what this block does
  # if (is.null(fns)) {
  #   if (!is.null(names)) {
  #     glue_mask <- across_glue_mask(.caller_env, .col = names_vars, .fn = "1")
  #     names <- fix_call(
  #       vec_as_names(glue(names, .envir = glue_mask), repair = "check_unique"),
  #       call = call(across_if_fn)
  #     )
  #   } else {
  #     names <- names_vars
  #   }
  #
  #   value <- list(vars = vars, fns = fns, names = names)
  #   return(value)
  # }

  # apply `.names` smart default
  if (is.function(fns) || is_formula(fns) || is.name(fns)) {
    names <- names %||% "{.col}"
    fns <- list("1" = fns)
  } else {
    names <- names %||% "{.col}_{.fn}"
    fns <- call_args(fns)
  }

  if (!is.list(fns)) {
    msg <- c("`.fns` must be NULL, a function, a formula, or a list of functions/formulas.")
    abort(msg, call = call(across_if_fn))
  }

  # make sure fns has names, use number to replace unnamed
  if (is.null(names(fns))) {
    names_fns <- seq_along(fns)
  } else {
    names_fns <- names(fns)
    empties <- which(names_fns == "")
    if (length(empties)) {
      names_fns[empties] <- empties
    }
  }

  glue_mask <- across_glue_mask(.caller_env,
    .col = rep(vars, each = length(fns)),
    .fn  = rep(names_fns , length(vars))
  )
  names <- vctrs::vec_as_names(glue::glue(names, .envir = glue_mask), repair = "check_unique")

  if (!inline) {
    fns <- map(fns, as_function)
  }

  list(vars = vars, fns = fns, names = names)
}

across_glue_mask <- function(.col, .fn, .caller_env) {
  glue_mask <- env(.caller_env, .col = .col, .fn = .fn)
  env_bind_active(
    glue_mask, col = function() glue_mask$.col, fn = function() glue_mask$.fn
  )
  glue_mask
}
