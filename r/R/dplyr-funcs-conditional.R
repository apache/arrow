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

#' Parse logical condition formulas
#'
#' Converts condition ~ value formulas into Arrow expressions. Unlike
#' `parse_value_mapping()`, the LHS must be a logical expression (not a value
#' to match against).
#'
#' @param formulas A list of two-sided formulas where LHS is a logical condition
#'   and RHS is the value to use when TRUE (e.g., `x > 5 ~ "high"`).
#' @param mask The data mask for evaluating formula expressions.
#'
#' @return A list with `query` (list of logical expressions) and `value`
#'   (list of replacement expressions).
#'
#' @keywords internal
#' @noRd
parse_condition_formulas <- function(formulas, mask) {
  fn <- call_name(rlang::caller_call())
  # Compact NULL entries (allows conditional formulas like: if (cond) x ~ y)
  formulas <- compact(formulas)
  n <- length(formulas)
  query <- vector("list", n)
  value <- vector("list", n)
  # Process each formula: condition ~ value
  for (i in seq_len(n)) {
    f <- formulas[[i]]
    if (!is_formula(f, lhs = TRUE)) {
      validation_error(paste0("Each argument to ", fn, "() must be a two-sided formula"))
    }
    # f[[2]] is LHS (logical condition), f[[3]] is RHS (value when TRUE)
    query[[i]] <- arrow_eval(f[[2]], mask)
    value[[i]] <- arrow_eval(f[[3]], mask)
    # Validate LHS is logical (unlike parse_value_mapping which does equality matching)
    if (!call_binding("is.logical", query[[i]])) {
      validation_error(paste0("Left side of each formula in ", fn, "() must be a logical expression"))
    }
  }
  list(query = query, value = value)
}

#' Create case_when Expression from query/value lists
#' @param query List of logical Arrow Expressions.
#' @param value List of value Arrow Expressions.
#' @return An Arrow Expression representing the case_when.
#' @keywords internal
#' @noRd
build_case_when_expr <- function(query, value) {
  Expression$create(
    "case_when",
    args = c(
      Expression$create(
        "make_struct",
        args = query,
        options = list(field_names = as.character(seq_along(query)))
      ),
      value
    )
  )
}

#' Build a match expression for x against a value (scalar, NA, or vector).
#' @param x Arrow Expression for the column to match against.
#' @param match_value Value to match - R scalar, vector, or NA. Expressions
#'   are compared with equality.
#' @return Arrow Expression that is TRUE when x matches match_value.
#' @keywords internal
#' @noRd
build_match_expr <- function(x, match_value) {
  # Expressions or length-1 non-NA: use equality directly
  if (inherits(match_value, "Expression") || length(match_value) == 1 && !is.na(match_value)) {
    return(x == match_value)
  }

  # R scalar NA requires is.na() since x == NA returns NA in Arrow
  if (length(match_value) == 1) {
    return(call_binding("is.na", x))
  }

  # R vector: use %in%, handling NA separately if present
  has_na <- any(is.na(match_value))
  non_na_values <- match_value[!is.na(match_value)]

  if (length(non_na_values) == 0) {
    call_binding("is.na", x)
  } else if (has_na) {
    call_binding("%in%", x, non_na_values) | call_binding("is.na", x)
  } else {
    call_binding("%in%", x, match_value)
  }
}

#' Build query/value lists from parallel from/to vectors.
#' NA values in `from` use is.na() for matching.
#' @param x Arrow Expression for the column to match against.
#' @param from Vector of values to match.
#' @param to Vector of replacement values (recycled to length of `from`).
#' @return list(query, value) for use with build_case_when_expr().
#' @keywords internal
#' @noRd
parse_from_to_mapping <- function(x, from, to) {
  n <- length(from)
  to <- vctrs::vec_recycle(to, n)
  query <- map(from, ~ build_match_expr(x, .x))
  value <- map(to, Expression$scalar)
  list(query = query, value = value)
}

#' Build query/value lists from value ~ replacement formulas.
#' NA values on LHS use is.na() for matching.
#' @param x Arrow Expression for the column to match against.
#' @param formulas List of two-sided formulas (value ~ replacement).
#' @param mask Data mask for evaluating formula expressions.
#' @param fn Calling function name (for error messages).
#' @return list(query, value) for use with build_case_when_expr().
#' @keywords internal
#' @noRd
parse_formula_mapping <- function(x, formulas, mask, fn) {
  # Compact NULL entries (allows conditional formulas like: if (cond) x ~ y)
  formulas <- compact(formulas)
  n <- length(formulas)
  query <- vector("list", n)
  value <- vector("list", n)
  for (i in seq_len(n)) {
    f <- formulas[[i]]
    if (!is_formula(f, lhs = TRUE)) {
      validation_error(paste0("Each argument to ", fn, "() must be a two-sided formula"))
    }
    # f[[2]] is LHS (value to match), f[[3]] is RHS (replacement)
    lhs <- arrow_eval(f[[2]], mask)
    query[[i]] <- build_match_expr(x, lhs)
    value[[i]] <- arrow_eval(f[[3]], mask)
  }
  list(query = query, value = value)
}

#' Dispatch to formula or from/to parser based on which args are provided.
#' Returns list(query, value) or NULL if no mappings.
#' @param x Arrow Expression for the column to match against.
#' @param formulas List of two-sided formulas (value ~ replacement).
#' @param from Vector of values to match (alternative to formulas).
#' @param to Vector of replacement values (used with `from`).
#' @param mask The data mask for evaluating formula expressions.
#' @keywords internal
#' @noRd
parse_value_mapping <- function(x, formulas = list(), from = NULL, to = NULL, mask) {
  fn <- call_name(rlang::caller_call())
  # Mutually exclusive interfaces
  if (length(formulas) > 0 && !is.null(from)) {
    validation_error(paste0("Can't use both `...` and `from`/`to` in ", fn, "()"))
  }

  if (length(formulas) > 0) {
    parse_formula_mapping(x, formulas, mask, fn)
  } else if (!is.null(from)) {
    if (is.null(to)) {
      validation_error("`to` must be provided when using `from`")
    }
    parse_from_to_mapping(x, from, to)
  } else {
    # No mappings provided
    NULL
  }
}

register_bindings_conditional <- function() {
  register_binding("%in%", function(x, table) {
    # We use `is_in` here, unlike with Arrays, which use `is_in_meta_binary`
    value_set <- Array$create(table)
    # If possible, `table` should be the same type as `x`
    # Try downcasting here; otherwise Acero may upcast x to table's type
    x_type <- x$type()
    # GH-43440: `is_in` doesn't want a DictionaryType in the value_set,
    # so we'll cast to its value_type
    # TODO: should this be pushed into cast_or_parse? Is this a bigger issue?
    if (inherits(x_type, "DictionaryType")) {
      x_type <- x_type$value_type
    }
    try(
      value_set <- cast_or_parse(value_set, x_type),
      silent = !getOption("arrow.debug", FALSE)
    )

    expr <- Expression$create(
      "is_in",
      x,
      options = list(
        value_set = value_set,
        skip_nulls = TRUE
      )
    )
  })

  register_binding("dplyr::coalesce", function(...) {
    args <- list2(...)
    if (length(args) < 1) {
      validation_error("At least one argument must be supplied to coalesce()")
    }

    # Treat NaN like NA for consistency with dplyr::coalesce(), but if *all*
    # the values are NaN, we should return NaN, not NA, so don't replace
    # NaN with NA in the final (or only) argument
    # TODO: if an option is added to the coalesce kernel to treat NaN as NA,
    # use that to simplify the code here (ARROW-13389)
    attr(args[[length(args)]], "last") <- TRUE
    args <- lapply(args, function(arg) {
      last_arg <- is.null(attr(arg, "last"))
      attr(arg, "last") <- NULL

      if (!inherits(arg, "Expression")) {
        arg <- Expression$scalar(arg)
      }

      if (last_arg && arg$type_id() %in% TYPES_WITH_NAN) {
        # store the NA_real_ in the same type as arg to avoid casting
        # smaller float types to larger float types
        NA_expr <- Expression$scalar(Scalar$create(NA_real_, type = arg$type()))
        Expression$create("if_else", Expression$create("is_nan", arg), NA_expr, arg)
      } else {
        arg
      }
    })
    Expression$create("coalesce", args = args)
  })

  # Although base R ifelse allows `yes` and `no` to be different classes
  register_binding("base::ifelse", function(test, yes, no) {
    args <- list(test, yes, no)
    # For if_else, the first arg should be a bool Expression, and we don't
    # want to consider that when casting the other args to the same type.
    # But ideally `yes` and `no` args should be the same type.
    args[-1] <- cast_scalars_to_common_type(args[-1])

    Expression$create("if_else", args = args)
  })

  register_binding("dplyr::if_else", function(condition, true, false, missing = NULL) {
    out <- call_binding("base::ifelse", condition, true, false)
    if (!is.null(missing)) {
      out <- call_binding(
        "base::ifelse",
        call_binding("is.na", condition),
        missing,
        out
      )
    }
    out
  })

  register_binding("dplyr::when_any", function(..., na_rm = FALSE, size = NULL) {
    if (!is.null(size)) {
      arrow_not_supported("`when_any()` with `size` specified")
    }
    args <- list2(...)
    if (na_rm) {
      args <- map(args, ~ call_binding("coalesce", .x, FALSE))
    }
    reduce(args, `|`)
  })

  register_binding("dplyr::when_all", function(..., na_rm = FALSE, size = NULL) {
    if (!is.null(size)) {
      arrow_not_supported("`when_all()` with `size` specified")
    }
    args <- list2(...)
    if (na_rm) {
      args <- map(args, ~ call_binding("coalesce", .x, TRUE))
    }
    reduce(args, `&`)
  })

  register_binding(
    "dplyr::case_when",
    function(..., .default = NULL, .ptype = NULL, .size = NULL) {
      if (!is.null(.ptype)) {
        arrow_not_supported("`case_when()` with `.ptype` specified")
      }

      if (!is.null(.size)) {
        arrow_not_supported("`case_when()` with `.size` specified")
      }

      formulas <- list2(...)
      if (length(formulas) == 0) {
        validation_error("No cases provided")
      }
      parsed <- parse_condition_formulas(formulas, caller_env())
      query <- parsed$query
      value <- parsed$value
      if (!is.null(.default)) {
        if (length(.default) != 1) {
          arrow_not_supported("`.default` must be size 1; vectors of length > 1")
        }
        n <- length(query)
        query[[n + 1]] <- TRUE
        value[[n + 1]] <- .default
      }
      build_case_when_expr(query, value)
    },
    notes = "`.ptype` and `.size` arguments not supported"
  )

  register_binding("dplyr::replace_when", function(x, ...) {
    formulas <- list2(...)
    if (length(formulas) == 0) {
      return(x)
    }
    parsed <- parse_condition_formulas(formulas, caller_env())
    query <- parsed$query
    value <- parsed$value
    n <- length(query)
    query[[n + 1]] <- TRUE
    value[[n + 1]] <- x
    build_case_when_expr(query, value)
  })

  register_binding("dplyr::replace_values", function(x, ..., from = NULL, to = NULL) {
    parsed <- parse_value_mapping(x, list2(...), from, to, caller_env())
    if (is.null(parsed)) {
      return(x)
    }
    query <- parsed$query
    value <- parsed$value
    n <- length(query)
    query[[n + 1]] <- TRUE
    value[[n + 1]] <- x
    build_case_when_expr(query, value)
  })

  register_binding(
    "dplyr::recode_values",
    function(x, ..., from = NULL, to = NULL, default = NULL, unmatched = "default", ptype = NULL) {
      if (!is.null(ptype)) {
        arrow_not_supported("`recode_values()` with `ptype` specified")
      }
      if (unmatched != "default") {
        arrow_not_supported('`recode_values()` with `unmatched` other than "default"')
      }

      parsed <- parse_value_mapping(x, list2(...), from, to, caller_env())
      if (is.null(parsed)) {
        validation_error("`...` can't be empty")
      }
      query <- parsed$query
      value <- parsed$value

      if (!is.null(default)) {
        if (length(default) != 1) {
          arrow_not_supported("`default` must be size 1; vectors of length > 1")
        }
        n <- length(query)
        query[[n + 1]] <- TRUE
        value[[n + 1]] <- Expression$scalar(default)
      }
      build_case_when_expr(query, value)
    },
    notes = "`ptype` argument and `unmatched = \"error\"` not supported"
  )
}
