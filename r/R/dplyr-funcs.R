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


#' @include expression.R
NULL

# This environment contains a cache of nse_funcs as a list()
.cache <- new.env(parent = emptyenv())

# Called in .onLoad()
create_translation_cache <- function() {
  arrow_funcs <- list()

  # Register all available Arrow Compute functions, namespaced as arrow_fun.
  if (arrow_available()) {
    all_arrow_funs <- list_compute_functions()
    arrow_funcs <- set_names(
      lapply(all_arrow_funs, function(fun) {
        force(fun)
        function(...) build_expr(fun, ...)
      }),
      paste0("arrow_", all_arrow_funs)
    )
  }

  # Register translations into nse_funcs and agg_funcs
  register_array_function_map_translations()
  register_aggregate_translations()
  register_conditional_translations()
  register_datetime_translations()
  register_math_translations()
  register_string_translations()
  register_type_translations()

  # We only create the cache for nse_funcs and not agg_funcs
  .cache$functions <- c(as.list(nse_funcs), arrow_funcs)
}

# nse_funcs is a list of functions that operated on (and return) Expressions
# These will be the basis for a data_mask inside dplyr methods
# and will be added to .cache at package load time
nse_funcs <- new.env(parent = emptyenv())

# agg_funcs is a list of functions with a different signature than nse_funcs;
# described below
agg_funcs <- new.env(parent = emptyenv())

translation_registry <- function() {
  nse_funcs
}

translation_registry_agg <- function() {
  agg_funcs
}

register_translation <- function(fun_name, fun, registry = translation_registry()) {
  name <- gsub("^.*?::", "", fun_name)
  namespace <- gsub("::.*$", "", fun_name)

  previous_fun <- if (name %in% names(fun)) registry[[name]] else NULL

  if (is.null(fun)) {
    rm(list = name, envir = registry)
  } else {
    registry[[name]] <- fun
  }

  invisible(previous_fun)
}

register_translation_agg <- function(fun_name, fun, registry = translation_registry_agg()) {
  register_translation(fun_name, fun, registry = registry)
}

# Start with mappings from R function name spellings
register_array_function_map_translations <- function() {
  # use a function to generate the binding so that `operator` persists
  # beyond execution time (another option would be to use quasiquotation
  # and unquote `operator` directly into the function expression)
  array_function_map_factory <- function(operator) {
    force(operator)
    function(...) build_expr(operator, ...)
  }

  for (name in names(.array_function_map)) {
    register_translation(name, array_function_map_factory(name))
  }
}

# Now add functions to that list where the mapping from R to Arrow isn't 1:1
# Each of these functions should have the same signature as the R function
# they're replacing.
#
# When to use `build_expr()` vs. `Expression$create()`?
#
# Use `build_expr()` if you need to
# (1) map R function names to Arrow C++ functions
# (2) wrap R inputs (vectors) as Array/Scalar
#
# `Expression$create()` is lower level. Most of the functions below use it
# because they manage the preparation of the user-provided inputs
# and don't need to wrap scalars


# Aggregation functions
# These all return a list of:
# @param fun string function name
# @param data Expression (these are all currently a single field)
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
  args[[1]]
}

agg_fun_output_type <- function(fun, input_type, hash) {
  # These are quick and dirty heuristics.
  if (fun %in% c("any", "all")) {
    bool()
  } else if (fun %in% "sum") {
    # It may upcast to a bigger type but this is close enough
    input_type
  } else if (fun %in% c("mean", "stddev", "variance", "approximate_median")) {
    float64()
  } else if (fun %in% "tdigest") {
    if (hash) {
      fixed_size_list_of(float64(), 1L)
    } else {
      float64()
    }
  } else {
    # Just so things don't error, assume the resulting type is the same
    input_type
  }
}

register_aggregate_translations <- function() {

  agg_funcs$sum <- function(..., na.rm = FALSE) {
    list(
      fun = "sum",
      data = ensure_one_arg(list2(...), "sum"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  }
  agg_funcs$any <- function(..., na.rm = FALSE) {
    list(
      fun = "any",
      data = ensure_one_arg(list2(...), "any"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  }
  agg_funcs$all <- function(..., na.rm = FALSE) {
    list(
      fun = "all",
      data = ensure_one_arg(list2(...), "all"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  }
  agg_funcs$mean <- function(x, na.rm = FALSE) {
    list(
      fun = "mean",
      data = x,
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  }
  agg_funcs$sd <- function(x, na.rm = FALSE, ddof = 1) {
    list(
      fun = "stddev",
      data = x,
      options = list(skip_nulls = na.rm, min_count = 0L, ddof = ddof)
    )
  }
  agg_funcs$var <- function(x, na.rm = FALSE, ddof = 1) {
    list(
      fun = "variance",
      data = x,
      options = list(skip_nulls = na.rm, min_count = 0L, ddof = ddof)
    )
  }
  agg_funcs$quantile <- function(x, probs, na.rm = FALSE) {
    if (length(probs) != 1) {
      arrow_not_supported("quantile() with length(probs) != 1")
    }
    # TODO: Bind to the Arrow function that returns an exact quantile and remove
    # this warning (ARROW-14021)
    warn(
      "quantile() currently returns an approximate quantile in Arrow",
      .frequency = ifelse(is_interactive(), "once", "always"),
      .frequency_id = "arrow.quantile.approximate"
    )
    list(
      fun = "tdigest",
      data = x,
      options = list(skip_nulls = na.rm, q = probs)
    )
  }
  agg_funcs$median <- function(x, na.rm = FALSE) {
    # TODO: Bind to the Arrow function that returns an exact median and remove
    # this warning (ARROW-14021)
    warn(
      "median() currently returns an approximate median in Arrow",
      .frequency = ifelse(is_interactive(), "once", "always"),
      .frequency_id = "arrow.median.approximate"
    )
    list(
      fun = "approximate_median",
      data = x,
      options = list(skip_nulls = na.rm)
    )
  }
  agg_funcs$n_distinct <- function(..., na.rm = FALSE) {
    list(
      fun = "count_distinct",
      data = ensure_one_arg(list2(...), "n_distinct"),
      options = list(na.rm = na.rm)
    )
  }
  agg_funcs$n <- function() {
    list(
      fun = "sum",
      data = Expression$scalar(1L),
      options = list()
    )
  }
  agg_funcs$min <- function(..., na.rm = FALSE) {
    list(
      fun = "min",
      data = ensure_one_arg(list2(...), "min"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  }
  agg_funcs$max <- function(..., na.rm = FALSE) {
    list(
      fun = "max",
      data = ensure_one_arg(list2(...), "max"),
      options = list(skip_nulls = na.rm, min_count = 0L)
    )
  }
}
