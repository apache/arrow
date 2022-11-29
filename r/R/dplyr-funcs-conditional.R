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

register_bindings_conditional <- function() {
  register_binding("%in%", function(x, table) {
    # We use `is_in` here, unlike with Arrays, which use `is_in_meta_binary`
    value_set <- Array$create(table)
    # If possible, `table` should be the same type as `x`
    # Try downcasting here; otherwise Acero may upcast x to table's type
    try(
      value_set <- cast_or_parse(value_set, x$type()),
      silent = TRUE
    )

    expr <- Expression$create("is_in", x,
      options = list(
        value_set = value_set,
        skip_nulls = TRUE
      )
    )
  })

  register_binding("dplyr::coalesce", function(...) {
    args <- list2(...)
    if (length(args) < 1) {
      abort("At least one argument must be supplied to coalesce()")
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
        # store the NA_real_ in the same type as arg to avoid avoid casting
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

  register_binding("dplyr::case_when", function(...) {
    formulas <- list2(...)
    n <- length(formulas)
    if (n == 0) {
      abort("No cases provided in case_when()")
    }
    query <- vector("list", n)
    value <- vector("list", n)
    mask <- caller_env()
    for (i in seq_len(n)) {
      f <- formulas[[i]]
      if (!inherits(f, "formula")) {
        abort("Each argument to case_when() must be a two-sided formula")
      }
      query[[i]] <- arrow_eval(f[[2]], mask)
      value[[i]] <- arrow_eval(f[[3]], mask)
      if (!call_binding("is.logical", query[[i]])) {
        abort("Left side of each formula in case_when() must be a logical expression")
      }
      if (inherits(value[[i]], "try-error")) {
        abort(handle_arrow_not_supported(value[[i]], format_expr(f[[3]])))
      }
    }
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
  })
}
