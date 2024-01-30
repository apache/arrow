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

expand_across <- function(.data, quos_in, exclude_cols = NULL) {
  quos_out <- list()
  # retrieve items using their values to preserve naming of quos other than across
  for (quo_i in seq_along(quos_in)) {
    quo_in <- quos_in[quo_i]
    quo_expr <- quo_get_expr(quo_in[[1]])
    quo_env <- quo_get_env(quo_in[[1]])

    if (is_call(quo_expr, c("across", "if_any", "if_all"))) {
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

      setup <- across_setup(
        cols = !!as_quosure(cols, quo_env),
        fns = across_call[[".fns"]],
        names = across_call[[".names"]],
        .caller_env = quo_env,
        mask = .data,
        inline = TRUE,
        exclude_cols = exclude_cols
      )

      new_quos <- quosures_from_setup(setup, quo_env)

      quos_out <- append(quos_out, new_quos)
    } else {
      quos_out <- append(quos_out, quo_in)
    }

    if (is_call(quo_expr, "if_any")) {
      quos_out <- append(list(), purrr::reduce(quos_out, combine_if, op = "|", envir = quo_get_env(quos_out[[1]])))
    }

    if (is_call(quo_expr, "if_all")) {
      quos_out <- append(list(), purrr::reduce(quos_out, combine_if, op = "&", envir = quo_get_env(quos_out[[1]])))
    }
  }

  new_quosures(quos_out)
}

# takes multiple expressions and combines them with & or |
combine_if <- function(lhs, rhs, op, envir) {
  expr_text <- paste(
    expr_text(quo_get_expr(lhs)),
    expr_text(quo_get_expr(rhs)),
    sep = paste0(" ", op, " ")
  )

  expr <- parse_expr(expr_text)

  new_quosure(expr, envir)
}

# given a named list of functions and column names, create a list of new quosures
quosures_from_setup <- function(setup, quo_env) {
  if (!is.null(setup$fns)) {
    func_list_full <- rep(setup$fns, length(setup$vars))
    cols_list_full <- rep(setup$vars, each = length(setup$fns))

    # get new quosures
    new_quo_list <- map2(
      func_list_full, cols_list_full,
      ~ as_across_fn_call(.x, .y, quo_env)
    )
  } else {
    # if there's no functions, just map to variables themselves
    new_quo_list <- map(
      setup$vars,
      ~ quo_set_env(quo(!!sym(.x)), quo_env)
    )
  }

  set_names(new_quo_list, setup$names)
}

across_setup <- function(cols, fns, names, .caller_env, mask, inline = FALSE, exclude_cols = NULL) {
  cols <- enquo(cols)

  sim_df <- dplyr::select(as.data.frame(implicit_schema(mask)), !(!!exclude_cols))
  vars <- names(dplyr::select(sim_df, !!cols))

  if (is.null(fns)) {
    if (!is.null(names)) {
      glue_mask <- across_glue_mask(.caller_env, .col = vars, .fn = "1")
      names <- vctrs::vec_as_names(glue::glue(names, .envir = glue_mask), repair = "check_unique")
    } else {
      names <- vars
    }

    value <- list(vars = vars, fns = fns, names = names)
    return(value)
  }

  is_single_func <- function(fns) {
    # function calls with package, like base::round
    (is.call(fns) && fns[[1]] == as.name("::")) ||
      # purrr-style formulae
      is_formula(fns) ||
      # single anonymous function
      is_call(fns, "function") ||
      # any other length 1 function calls
      (length(fns) == 1 && (is.function(fns) || is_formula(fns) || is.name(fns)))
  }

  # apply `.names` smart default
  if (is_single_func(fns)) {
    names <- names %||% "{.col}"
    fns <- list("1" = fns)
  } else {
    names <- names %||% "{.col}_{.fn}"
    fns <- call_args(fns)
  }

  # ARROW-14071
  if (all(map_lgl(fns, is_call, name = "function"))) {
    abort("Anonymous functions are not yet supported in Arrow")
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
    .fn  = rep(names_fns, length(vars))
  )
  names <- vctrs::vec_as_names(glue::glue(names, .envir = glue_mask), repair = "check_unique")

  if (!inline) {
    fns <- map(fns, as_function)
  }

  # ensure .names argument has resulted in
  if (length(names) != (length(vars) * length(fns))) {
    abort(
      c(
        "`.names` specification must produce (number of columns * number of functions) names.",
        x = paste0(
          length(vars) * length(fns), " names required (", length(vars), " columns * ", length(fns), " functions)\n  ",
          length(names), " name(s) produced: ", paste(names, collapse = ",")
        )
      )
    )
  }

  list(vars = vars, fns = fns, names = names)
}

across_glue_mask <- function(.col, .fn, .caller_env) {
  env(.caller_env, .col = .col, .fn = .fn, col = .col, fn = .fn)
}

# Substitutes instances of "." and ".x" with `var`
as_across_fn_call <- function(fn, var, quo_env) {
  if (is_formula(fn, lhs = FALSE)) {
    expr <- f_rhs(fn)
    expr <- expr_substitute(expr, quote(.), sym(var))
    expr <- expr_substitute(expr, quote(.x), sym(var))
    new_quosure(expr, quo_env)
  } else {
    fn_call <- call2(fn, sym(var))
    new_quosure(fn_call, quo_env)
  }
}

expr_substitute <- function(expr, old, new) {
  switch(typeof(expr),
    language = {
      expr[] <- lapply(expr, expr_substitute, old, new)
      return(expr)
    },
    symbol = if (identical(expr, old)) {
      return(new)
    }
  )

  expr
}
