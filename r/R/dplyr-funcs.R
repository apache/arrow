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
