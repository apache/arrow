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

#' Register user-defined functions
#'
#' These functions support calling R code from query engine execution
#' (i.e., a [dplyr::mutate()] or [dplyr::filter()] on a [Table] or [Dataset]).
#' Use [register_scalar_function()] attach Arrow input and output types to an
#' R function and make it available for use in the dplyr interface and/or
#' [call_function()]. Scalar functions are currently the only type of
#' user-defined function supported. In Arrow, scalar functions must be
#' stateless and return output with the same shape (i.e., the same number
#' of rows) as the input.
#'
#' @param name The function name to be used in the dplyr bindings
#' @param in_type A [DataType] of the input type or a [schema()]
#'   for functions with more than one argument. This signature will be used
#'   to determine if this function is appropriate for a given set of arguments.
#'   If this function is appropriate for more than one signature, pass a
#'   `list()` of the above.
#' @param out_type A [DataType] of the output type or a function accepting
#'   a single argument (`types`), which is a `list()` of [DataType]s. If a
#'   function it must return a [DataType].
#' @param fun An R function or rlang-style lambda expression. The function
#'   will be called with a first argument `context` which is a `list()`
#'   with elements `batch_size` (the expected length of the output) and
#'   `output_type` (the required [DataType] of the output) that may be used
#'   to ensure that the output has the correct type and length. Subsequent
#'   arguments are passed by position as specified by `in_types`. If
#'   `auto_convert` is `TRUE`, subsequent arguments are converted to
#'   R vectors before being passed to `fun` and the output is automatically
#'   constructed with the expected output type via [as_arrow_array()].
#' @param auto_convert Use `TRUE` to convert inputs before passing to `fun`
#'   and construct an Array of the correct type from the output. Use this
#'   option to write functions of R objects as opposed to functions of
#'   Arrow R6 objects.
#'
#' @return `NULL`, invisibly
#' @export
#'
#' @examplesIf arrow_with_dataset() && identical(Sys.getenv("NOT_CRAN"), "true")
#' library(dplyr, warn.conflicts = FALSE)
#'
#' some_model <- lm(mpg ~ disp + cyl, data = mtcars)
#' register_scalar_function(
#'   "mtcars_predict_mpg",
#'   function(context, disp, cyl) {
#'     predict(some_model, newdata = data.frame(disp, cyl))
#'   },
#'   in_type = schema(disp = float64(), cyl = float64()),
#'   out_type = float64(),
#'   auto_convert = TRUE
#' )
#'
#' as_arrow_table(mtcars) %>%
#'   transmute(mpg, mpg_predicted = mtcars_predict_mpg(disp, cyl)) %>%
#'   collect() %>%
#'   head()
#'
register_scalar_function <- function(name, fun, in_type, out_type,
                                     auto_convert = FALSE) {
  assert_that(is.string(name))

  scalar_function <- arrow_scalar_function(
    fun,
    in_type,
    out_type,
    auto_convert = auto_convert
  )

  # register with Arrow C++ function registry (enables its use in
  # call_function() and Expression$create())
  RegisterScalarUDF(name, scalar_function)

  # register with dplyr binding (enables its use in mutate(), filter(), etc.)
  binding_fun <- function(...) Expression$create(name, ...)

  # inject the value of `name` into the expression to avoid saving this
  # execution environment in the binding, which eliminates a warning when the
  # same binding is registered twice
  body(binding_fun) <- expr_substitute(body(binding_fun), sym("name"), name)
  environment(binding_fun) <- asNamespace("arrow")

  register_binding(
    name,
    binding_fun,
    update_cache = TRUE
  )

  invisible(NULL)
}

arrow_scalar_function <- function(fun, in_type, out_type, auto_convert = FALSE) {
  assert_that(is.function(fun))

  # Create a small wrapper function that is easier to call from C++.
  # TODO(ARROW-17148): This wrapper could be implemented in C/C++ to
  # reduce evaluation overhead and generate prettier backtraces when
  # errors occur (probably using a similar approach to purrr).
  if (auto_convert) {
    wrapper_fun <- function(context, args) {
      args <- lapply(args, as.vector)
      result <- do.call(fun, c(list(context), args))
      as_arrow_array(result, type = context$output_type)
    }
  } else {
    wrapper_fun <- function(context, args) {
      do.call(fun, c(list(context), args))
    }
  }

  # in_type can be a list() if registering multiple kernels at once
  if (is.list(in_type)) {
    in_type <- lapply(in_type, in_type_as_schema)
  } else {
    in_type <- list(in_type_as_schema(in_type))
  }

  # out_type can be a list() if registering multiple kernels at once
  if (is.list(out_type)) {
    out_type <- lapply(out_type, out_type_as_function)
  } else {
    out_type <- list(out_type_as_function(out_type))
  }

  # recycle out_type (which is frequently length 1 even if multiple kernels
  # are being registered at once)
  out_type <- rep_len(out_type, length(in_type))

  # check n_kernels and number of args in fun
  n_kernels <- length(in_type)
  if (n_kernels == 0) {
    abort("Can't register user-defined scalar function with 0 kernels")
  }

  expected_n_args <- in_type[[1]]$num_fields + 1L
  fun_formals_have_dots <- any(names(formals(fun)) == "...")
  if (!fun_formals_have_dots && length(formals(fun)) != expected_n_args) {
    abort(
      sprintf(
        paste0(
          "Expected `fun` to accept %d argument(s)\n",
          "but found a function that acccepts %d argument(s)\n",
          "Did you forget to include `context` as the first argument?"
        ),
        expected_n_args,
        length(formals(fun))
      )
    )
  }

  structure(
    list(
      wrapper_fun = wrapper_fun,
      in_type = in_type,
      out_type = out_type
    ),
    class = "arrow_scalar_function"
  )
}

# This function sanitizes the in_type argument for arrow_scalar_function(),
# which can be a data type (e.g., int32()), a field for a unary function
# or a schema() for functions accepting more than one argument. C++ expects
# a schema().
in_type_as_schema <- function(x) {
  if (inherits(x, "Field")) {
    schema(x)
  } else if (inherits(x, "DataType")) {
    schema(field("", x))
  } else {
    as_schema(x)
  }
}

# This function sanitizes the out_type argument for arrow_scalar_function(),
# which can be a data type (e.g., int32()) or a function of the input types.
# C++ currently expects a function.
out_type_as_function <- function(x) {
  if (is.function(x)) {
    x
  } else {
    x <- as_data_type(x)
    function(types) x
  }
}
