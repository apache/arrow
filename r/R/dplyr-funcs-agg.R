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
#
# These all insert into an .aggregations list in the mask, a list containing:
# @param fun string function name
# @param data list of 0 or more Expressions
# @param options list of function options, as passed to call_function
# The functions return a FieldRef pointing to the result of the aggregation.
#
# For group-by aggregation, `hash_` gets prepended to the function name when
# the query is executed.
# So to see a list of available hash aggregation functions,
# you can use list_compute_functions("^hash_")

register_bindings_aggregate <- function() {
  register_binding("base::sum", function(..., na.rm = FALSE) {
    set_agg(
      fun = "sum",
      data = ensure_one_arg(list2(...), "sum"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding("base::prod", function(..., na.rm = FALSE) {
    set_agg(
      fun = "product",
      data = ensure_one_arg(list2(...), "prod"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding("base::any", function(..., na.rm = FALSE) {
    set_agg(
      fun = "any",
      data = ensure_one_arg(list2(...), "any"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding("base::all", function(..., na.rm = FALSE) {
    set_agg(
      fun = "all",
      data = ensure_one_arg(list2(...), "all"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding("base::mean", function(x, na.rm = FALSE) {
    set_agg(
      fun = "mean",
      data = list(x),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding("stats::sd", function(x, na.rm = FALSE, ddof = 1) {
    set_agg(
      fun = "stddev",
      data = list(x),
      options = list(skip_nulls = na.rm, min_count = 0L, ddof = ddof)
    )
  })
  register_binding("stats::var", function(x, na.rm = FALSE, ddof = 1) {
    set_agg(
      fun = "variance",
      data = list(x),
      options = list(skip_nulls = na.rm, min_count = 0L, ddof = ddof)
    )
  })
  register_binding(
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
  register_binding(
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
  register_binding("dplyr::n_distinct", function(..., na.rm = FALSE) {
    set_agg(
      fun = "count_distinct",
      data = ensure_one_arg(list2(...), "n_distinct"),
      options = list(na.rm = na.rm)
    )
  })
  register_binding("dplyr::n", function() {
    set_agg(
      fun = "count_all",
      data = list(),
      options = list()
    )
  })
  register_binding("base::min", function(..., na.rm = FALSE) {
    set_agg(
      fun = "min",
      data = ensure_one_arg(list2(...), "min"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding("base::max", function(..., na.rm = FALSE) {
    set_agg(
      fun = "max",
      data = ensure_one_arg(list2(...), "max"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  })
  register_binding("arrow::one", one)
}

set_agg <- function(...) {
  agg_data <- list2(...)
  # Find the environment where .aggregations is stored
  target <- find_arrow_mask()
  aggs <- get(".aggregations", target)
  lapply(agg_data[["data"]], function(expr) {
    # If any of the fields referenced in the expression are in .aggregations,
    # then we can't aggregate over them.
    # This is mainly for combinations of dataset columns and aggregations,
    # like sum(x - mean(x)), i.e. window functions.
    # This will reject (sum(sum(x)) as well, but that's not a useful operation.
    if (any(expr$field_names_in_expression() %in% names(aggs))) {
      arrow_not_supported("aggregate within aggregate expression")
    }
  })

  # Record the (fun, data, options) in .aggregations
  # and return a FieldRef pointing to it
  tmpname <- paste0("..temp", length(aggs))
  aggs[[tmpname]] <- agg_data
  assign(".aggregations", aggs, envir = target)
  Expression$field_ref(tmpname)
}

find_arrow_mask <- function() {
  # Find the arrow_mask environment by looking for .aggregations,
  # it's in parent.env of something in the call stack
  n <- 1
  while (TRUE) {
    if (exists(".aggregations", envir = caller_env(n))) {
      return(caller_env(n))
    }
    n <- n + 1
  }
}
#' Get one value from each group
#'
#' Returns one arbitrary value from the input for each group. The function is
#' biased towards non-null values: if there is at least one non-null value for a
#' certain group, that value is returned, and only if all the values are null
#' for the group will the function return null.
#'
#' @param ... Unquoted column name to pull values from.
#'
#' @examples
#' \dontrun{
#' mtcars |>
#'   arrow_table() |>
#'   group_by(cyl) |>
#'   summarize(x = one(disp))
#' }
#' @keywords internal
one <- function(...) {
  set_agg(
    fun = "one",
    data = ensure_one_arg(list2(...), "one"),
    options = list()
  )
}

ensure_one_arg <- function(args, fun) {
  if (length(args) == 0) {
    arrow_not_supported(paste0(fun, "() with 0 arguments"))
  } else if (length(args) > 1) {
    arrow_not_supported(paste0("Multiple arguments to ", fun, "()"))
  }
  args
}
