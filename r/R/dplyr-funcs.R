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


#' Register compute bindings
#'
#' The `register_binding()` and `register_binding_agg()` functions
#' are used to populate a list of functions that operate on (and return)
#' Expressions. These are the basis for the `.data` mask inside dplyr methods.
#'
#' @section Writing bindings:
#' * `Expression$create()` will wrap any non-Expression inputs as Scalar
#'   Expressions. If you want to try to coerce scalar inputs to match the type
#'   of the Expression(s) in the arguments, call
#'  `cast_scalars_to_common_type(args)` on the
#'   args. For example, `Expression$create("add", args = list(int16_field, 1))`
#'   would result in a `float64` type output because `1` is a `double` in R.
#'   To prevent casting all of the data in `int16_field` to float and to
#'   preserve it as int16, do
#'   `Expression$create("add",
#'   args = cast_scalars_to_common_type(list(int16_field, 1)))`
#' * Inside your function, you can call any other binding with `call_binding()`.
#'
#' @param fun_name A string containing a function name in the form `"function"` or
#'   `"package::function"`. The package name is currently not used but
#'   may be used in the future to allow these types of function calls.
#' @param fun A function or `NULL` to un-register a previous function.
#'   This function must accept `Expression` objects as arguments and return
#'   `Expression` objects instead of regular R objects.
#' @param agg_fun An aggregate function or `NULL` to un-register a previous
#'   aggregate function. This function must accept `Expression` objects as
#'   arguments and return a `list()` with components:
#'   - `fun`: string function name
#'   - `data`: list of 0 or more `Expression`s
#'   - `options`: list of function options, as passed to call_function
#' @param update_cache Update .cache$functions at the time of registration.
#'   the default is FALSE because the majority of usage is to register
#'   bindings at package load, after which we create the cache once. The
#'   reason why .cache$functions is needed in addition to nse_funcs for
#'   non-aggregate functions could be revisited...it is currently used
#'   as the data mask in mutate, filter, and aggregate (but not
#'   summarise) because the data mask has to be a list.
#' @param registry An environment in which the functions should be
#'   assigned.
#' @param notes string for the docs: note any limitations or differences in
#'   behavior between the Arrow version and the R function.
#' @return The previously registered binding or `NULL` if no previously
#'   registered function existed.
#' @keywords internal
register_binding <- function(fun_name,
                             fun,
                             registry = nse_funcs,
                             update_cache = FALSE,
                             notes = character(0)) {
  unqualified_name <- sub("^.*?:{+}", "", fun_name)

  previous_fun <- registry[[unqualified_name]]

  # if the unqualified name exists in the registry, warn
  if (!is.null(previous_fun) && !identical(fun, previous_fun)) {
    warn(
      paste0(
        "A \"",
        unqualified_name,
        "\" binding already exists in the registry and will be overwritten."
      )
    )
  }

  # register both as `pkg::fun` and as `fun` if `qualified_name` is prefixed
  # unqualified_name and fun_name will be the same if not prefixed
  registry[[unqualified_name]] <- fun
  registry[[fun_name]] <- fun

  .cache$docs[[fun_name]] <- notes

  if (update_cache) {
    fun_cache <- .cache$functions
    fun_cache[[unqualified_name]] <- fun
    fun_cache[[fun_name]] <- fun
    .cache$functions <- fun_cache
  }

  invisible(previous_fun)
}

unregister_binding <- function(fun_name, registry = nse_funcs,
                               update_cache = FALSE) {
  unqualified_name <- sub("^.*?:{+}", "", fun_name)
  previous_fun <- registry[[unqualified_name]]

  rm(
    list = unique(c(fun_name, unqualified_name)),
    envir = registry,
    inherits = FALSE
  )

  if (update_cache) {
    fun_cache <- .cache$functions
    fun_cache[[unqualified_name]] <- NULL
    fun_cache[[fun_name]] <- NULL
    .cache$functions <- fun_cache
  }

  invisible(previous_fun)
}

register_binding_agg <- function(fun_name,
                                 agg_fun,
                                 registry = agg_funcs,
                                 notes = character(0)) {
  register_binding(fun_name, agg_fun, registry = registry, notes = notes)
}

# Supports functions and tests that call previously-defined bindings
call_binding <- function(fun_name, ...) {
  nse_funcs[[fun_name]](...)
}

call_binding_agg <- function(fun_name, ...) {
  agg_funcs[[fun_name]](...)
}

create_binding_cache <- function() {
  # Called in .onLoad()
  .cache$docs <- list()

  # Register all available Arrow Compute functions, namespaced as arrow_fun.
  all_arrow_funs <- list_compute_functions()
  arrow_funcs <- set_names(
    lapply(all_arrow_funs, function(fun) {
      force(fun)
      function(...) Expression$create(fun, ...)
    }),
    paste0("arrow_", all_arrow_funs)
  )

  # Register bindings into nse_funcs and agg_funcs
  register_bindings_array_function_map()
  register_bindings_aggregate()
  register_bindings_conditional()
  register_bindings_datetime()
  register_bindings_math()
  register_bindings_string()
  register_bindings_type()
  register_bindings_augmented()

  # We only create the cache for nse_funcs and not agg_funcs
  .cache$functions <- c(as.list(nse_funcs), arrow_funcs)
}

# environments in the arrow namespace used in the above functions
nse_funcs <- new.env(parent = emptyenv())
agg_funcs <- new.env(parent = emptyenv())
.cache <- new.env(parent = emptyenv())

# we register 2 versions of the "::" binding - one for use with nse_funcs
# (registered below) and another one for use with agg_funcs (registered in
# dplyr-summarize.R)
nse_funcs[["::"]] <- function(lhs, rhs) {
  lhs_name <- as.character(substitute(lhs))
  rhs_name <- as.character(substitute(rhs))

  fun_name <- paste0(lhs_name, "::", rhs_name)

  # if we do not have a binding for pkg::fun, then fall back on to the
  # regular pkg::fun function
  nse_funcs[[fun_name]] %||% asNamespace(lhs_name)[[rhs_name]]
}
