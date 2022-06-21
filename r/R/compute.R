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

#' Call an Arrow compute function
#'
#' This function provides a lower-level API for calling Arrow functions by their
#' string function name. You won't use it directly for most applications.
#' Many Arrow compute functions are mapped to R methods,
#' and in a `dplyr` evaluation context, [all Arrow functions][list_compute_functions()]
#' are callable with an `arrow_` prefix.
#' @param function_name string Arrow compute function name
#' @param ... Function arguments, which may include `Array`, `ChunkedArray`, `Scalar`,
#' `RecordBatch`, or `Table`.
#' @param args list arguments as an alternative to specifying in `...`
#' @param options named list of C++ function options.
#' @details When passing indices in `...`, `args`, or `options`, express them as
#' 0-based integers (consistent with C++).
#' @return An `Array`, `ChunkedArray`, `Scalar`, `RecordBatch`, or `Table`, whatever the compute function results in.
#' @seealso [Arrow C++ documentation](https://arrow.apache.org/docs/cpp/compute.html) for
#'   the functions and their respective options.
#' @examples
#' a <- Array$create(c(1L, 2L, 3L, NA, 5L))
#' s <- Scalar$create(4L)
#' call_function("coalesce", a, s)
#'
#' a <- Array$create(rnorm(10000))
#' call_function("quantile", a, options = list(q = seq(0, 1, 0.25)))
#' @export
#' @include array.R
#' @include chunked-array.R
#' @include scalar.R
call_function <- function(function_name, ..., args = list(...), options = empty_named_list()) {
  assert_that(is.string(function_name))
  assert_that(is.list(options), !is.null(names(options)))

  datum_classes <- c("Array", "ChunkedArray", "RecordBatch", "Table", "Scalar")
  valid_args <- map_lgl(args, ~ inherits(., datum_classes))
  if (!all(valid_args)) {
    # Lame, just pick one to report
    first_bad <- min(which(!valid_args))
    stop(
      "Argument ", first_bad, " is of class ", head(class(args[[first_bad]]), 1),
      " but it must be one of ", oxford_paste(datum_classes, "or"),
      call. = FALSE
    )
  }

  compute__CallFunction(function_name, args, options)
}

#' List available Arrow C++ compute functions
#'
#' This function lists the names of all available Arrow C++ library compute functions.
#' These can be called by passing to [call_function()], or they can be
#' called by name with an `arrow_` prefix inside a `dplyr` verb.
#'
#' The resulting list describes the capabilities of your `arrow` build.
#' Some functions, such as string and regular expression functions,
#' require optional build-time C++ dependencies. If your `arrow` package
#' was not compiled with those features enabled, those functions will
#' not appear in this list.
#'
#' Some functions take options that need to be passed when calling them
#' (in a list called `options`). These options require custom handling
#' in C++; many functions already have that handling set up but not all do.
#' If you encounter one that needs special handling for options, please
#' report an issue.
#'
#' Note that this list does *not* enumerate all of the R bindings for these functions.
#' The package includes Arrow methods for many base R functions that can
#' be called directly on Arrow objects, as well as some tidyverse-flavored versions
#' available inside `dplyr` verbs.
#'
#' @param pattern Optional regular expression to filter the function list
#' @param ... Additional parameters passed to `grep()`
#' @return A character vector of available Arrow C++ function names
#' @examples
#' available_funcs <- list_compute_functions()
#' utf8_funcs <- list_compute_functions(pattern = "^UTF8", ignore.case = TRUE)
#' @export
list_compute_functions <- function(pattern = NULL, ...) {
  funcs <- compute__GetFunctionNames()
  if (!is.null(pattern)) {
    funcs <- grep(pattern, funcs, value = TRUE, ...)
  }
  funcs <- grep(
    "^hash_",
    funcs,
    value = TRUE,
    invert = TRUE
  )
  funcs
}

#' @export
sum.ArrowDatum <- function(..., na.rm = FALSE) {
  scalar_aggregate("sum", ..., na.rm = na.rm)
}

#' @export
mean.ArrowDatum <- function(..., na.rm = FALSE) {
  scalar_aggregate("mean", ..., na.rm = na.rm)
}

#' @export
min.ArrowDatum <- function(..., na.rm = FALSE) {
  scalar_aggregate("min_max", ..., na.rm = na.rm)$GetFieldByName("min")
}

#' @export
max.ArrowDatum <- function(..., na.rm = FALSE) {
  scalar_aggregate("min_max", ..., na.rm = na.rm)$GetFieldByName("max")
}

scalar_aggregate <- function(FUN, ..., na.rm = FALSE, min_count = 0L) {
  a <- collect_arrays_from_dots(list(...))
  if (FUN == "min_max" && na.rm && a$null_count == length(a)) {
    Array$create(data.frame(min = Inf, max = -Inf))
    # If na.rm == TRUE and all values in array are NA, R returns
    # Inf/-Inf, which are type double. Since Arrow is type-stable
    # and does not do that, we handle this special case here.
  } else {
    call_function(FUN, a, options = list(skip_nulls = na.rm, min_count = min_count))
  }
}

collect_arrays_from_dots <- function(dots) {
  # Given a list that may contain both Arrays and ChunkedArrays,
  # return a single ChunkedArray containing all of those chunks
  # (may return a regular Array if there is only one element in dots)
  # If there is only one element and it is a scalar, it returns the scalar
  if (length(dots) == 1) {
    return(dots[[1]])
  }

  assert_that(all(map_lgl(dots, is.Array)))
  arrays <- unlist(lapply(dots, function(x) {
    if (inherits(x, "ChunkedArray")) {
      x$chunks
    } else {
      x
    }
  }))
  ChunkedArray$create(!!!arrays)
}

#' @export
quantile.ArrowDatum <- function(x,
                                probs = seq(0, 1, 0.25),
                                na.rm = FALSE,
                                type = 7,
                                interpolation = c("linear", "lower", "higher", "nearest", "midpoint"),
                                ...) {
  if (inherits(x, "Scalar")) x <- Array$create(x)
  assert_is(probs, c("numeric", "integer"))
  assert_that(length(probs) > 0)
  assert_that(all(probs >= 0 & probs <= 1))
  if (!na.rm && x$null_count > 0) {
    stop("Missing values not allowed if 'na.rm' is FALSE", call. = FALSE)
  }
  if (type != 7) {
    stop(
      "Argument `type` not supported in Arrow. To control the quantile ",
      "interpolation algorithm, set argument `interpolation` to one of: ",
      "\"linear\" (the default), \"lower\", \"higher\", \"nearest\", or ",
      "\"midpoint\".",
      call. = FALSE
    )
  }
  interpolation <- QuantileInterpolation[[toupper(match.arg(interpolation))]]
  out <- call_function("quantile", x, options = list(q = probs, interpolation = interpolation))
  if (length(out) == 0) {
    # When there are no non-missing values in the data, the Arrow quantile
    # function returns an empty Array, but for consistency with the R quantile
    # function, we want an Array of NA_real_ with the same length as probs
    out <- Array$create(rep(NA_real_, length(probs)))
  }
  out
}

#' @export
median.ArrowDatum <- function(x, na.rm = FALSE, ...) {
  if (!na.rm && x$null_count > 0) {
    Scalar$create(NA_real_)
  } else {
    Scalar$create(quantile(x, probs = 0.5, na.rm = TRUE, ...))
  }
}

#' @export
unique.ArrowDatum <- function(x, incomparables = FALSE, ...) {
  call_function("unique", x)
}

#' @export
any.ArrowDatum <- function(..., na.rm = FALSE) {
  scalar_aggregate("any", ..., na.rm = na.rm)
}

#' @export
all.ArrowDatum <- function(..., na.rm = FALSE) {
  scalar_aggregate("all", ..., na.rm = na.rm)
}

#' `match` and `%in%` for Arrow objects
#'
#' `base::match()` is not a generic, so we can't just define Arrow methods for
#' it. This function exposes the analogous functions in the Arrow C++ library.
#'
#' @param x `Scalar`, `Array` or `ChunkedArray`
#' @param table `Scalar`, Array`, `ChunkedArray`, or R vector lookup table.
#' @param ... additional arguments, ignored
#' @return `match_arrow()` returns an `int32`-type Arrow object of the same length
#' and type as `x` with the (0-based) indexes into `table`. `is_in()` returns a
#' `boolean`-type Arrow object of the same length and type as `x` with values indicating
#' per element of `x` it it is present in `table`.
#' @examples
#' # note that the returned value is 0-indexed
#' cars_tbl <- arrow_table(name = rownames(mtcars), mtcars)
#' match_arrow(Scalar$create("Mazda RX4 Wag"), cars_tbl$name)
#'
#' is_in(Array$create("Mazda RX4 Wag"), cars_tbl$name)
#'
#' # Although there are multiple matches, you are returned the index of the first
#' # match, as with the base R equivalent
#' match(4, mtcars$cyl) # 1-indexed
#' match_arrow(Scalar$create(4), cars_tbl$cyl) # 0-indexed
#'
#' # If `x` contains multiple values, you are returned the indices of the first
#' # match for each value.
#' match(c(4, 6, 8), mtcars$cyl)
#' match_arrow(Array$create(c(4, 6, 8)), cars_tbl$cyl)
#'
#' # Return type matches type of `x`
#' is_in(c(4, 6, 8), mtcars$cyl) # returns vector
#' is_in(Scalar$create(4), mtcars$cyl) # returns Scalar
#' is_in(Array$create(c(4, 6, 8)), cars_tbl$cyl) # returns Array
#' is_in(ChunkedArray$create(c(4, 6), 8), cars_tbl$cyl) # returns ChunkedArray
#' @export
match_arrow <- function(x, table, ...) {
  if (!inherits(x, "ArrowDatum")) {
    x <- Array$create(x)
  }

  if (!inherits(table, c("Array", "ChunkedArray"))) {
    table <- Array$create(table)
  }
  call_function("index_in_meta_binary", x, table)
}

#' @rdname match_arrow
#' @export
is_in <- function(x, table, ...) {
  if (!inherits(x, "ArrowDatum")) {
    x <- Array$create(x)
  }

  if (!inherits(table, c("Array", "DictionaryArray", "ChunkedArray"))) {
    table <- Array$create(table)
  }
  call_function("is_in_meta_binary", x, table)
}

#' `table` for Arrow objects
#'
#' This function tabulates the values in the array and returns a table of counts.
#' @param x `Array` or `ChunkedArray`
#' @return A `StructArray` containing "values" (same type as `x`) and "counts"
#' `Int64`.
#' @examples
#' cyl_vals <- Array$create(mtcars$cyl)
#' counts <- value_counts(cyl_vals)
#' @export
value_counts <- function(x) {
  call_function("value_counts", x)
}

#' Cast options
#'
#' @param safe logical: enforce safe conversion? Default `TRUE`
#' @param ... additional cast options, such as `allow_int_overflow`,
#' `allow_time_truncate`, and `allow_float_truncate`, which are set to `!safe`
#' by default
#' @return A list
#' @export
#' @keywords internal
cast_options <- function(safe = TRUE, ...) {
  opts <- list(
    allow_int_overflow = !safe,
    allow_time_truncate = !safe,
    allow_float_truncate = !safe
  )
  modifyList(opts, list(...))
}

#' Register user-defined functions
#'
#' These functions support calling R code from query engine execution
#' (i.e., a [dplyr::mutate()] or [dplyr::filter()] on a [Table] or [Dataset]).
#' Use [arrow_scalar_function()] to define an R function that accepts and
#' returns R objects; use [arrow_base_scalar_function()] to define a
#' lower-level function that operates directly on Arrow objects.
#'
#' @param name The function name to be used in the dplyr bindings
#' @param scalar_function An object created with [arrow_scalar_function()]
#'   or [arrow_base_scalar_function()].
#' @param registry_name The function name to be used in the Arrow C++
#'   compute function registry. This may be different from `name`.
#' @param in_type A [DataType] of the input type or a [schema()]
#'   for functions with more than one argument. This signature will be used
#'   to determine if this function is appropriate for a given set of arguments.
#'   If this function is appropriate for more than one signature, pass a
#'   `list()` of the above.
#' @param out_type A [DataType] of the output type or a function accepting
#'   a single argument (`types`), which is a `list()` of [DataType]s. If a
#'   function it must return a [DataType].
#' @param fun An R function or rlang-style lambda expression. This function
#'   will be called with R objects as arguments and must return an object
#'   that can be converted to an [Array] using [as_arrow_array()]. Function
#'   authors must take care to return an array castable to the output data
#'   type specified by `out_type`.
#' @param base_fun An R function or rlang-style lambda expression. This
#'   function will be called with exactly two arguments: `kernel_context`,
#'   which is a `list()` of objects giving information about the
#'   execution context and `args`, which is a list of [Array] or [Scalar]
#'   objects corresponding to the input arguments.
#'
#' @return
#'   - `register_scalar_function()`: `NULL`, invisibly
#'   - `arrow_scalar_function()`: returns an object of class
#'     "arrow_base_scalar_function" that can be passed to
#'     `register_scalar_function()`.
#' @export
#'
#' @examples
#' fun_wrapper <- arrow_scalar_function(
#'   schema(x = float64(), y = float64(), z = float64()),
#'   float64(),
#'   function(x, y, z) x + y + z
#' )
#' register_scalar_function("example_add3", fun_wrapper)
#'
#' call_function(
#'   "example_add3",
#'   Scalar$create(1),
#'   Scalar$create(2),
#'   Array$create(3)
#' )
#'
#' # use arrow_base_scalar_function() for a lower-level interface
#' base_fun_wrapper <- arrow_base_scalar_function(
#'   schema(x = float64(), y = float64(), z = float64()),
#'   float64(),
#'   function(context, args) {
#'     args[[1]] + args[[2]] + args[[3]]
#'   }
#' )
#' register_scalar_function("example_add3", base_fun_wrapper)
#'
#' call_function(
#'   "example_add3",
#'   Scalar$create(1),
#'   Scalar$create(2),
#'   Array$create(3)
#' )
#'
register_scalar_function <- function(name, scalar_function, registry_name = name) {
  assert_that(
    is.string(name),
    is.string(registry_name),
    inherits(scalar_function, "arrow_base_scalar_function")
  )

  # register with Arrow C++
  RegisterScalarUDF(registry_name, scalar_function)

  # register with dplyr bindings
  register_binding(
    name,
    function(...) build_expr(registry_name, ...)
  )

  # recreate dplyr binding cache
  create_binding_cache()

  invisible(NULL)
}

#' @rdname register_scalar_function
#' @export
arrow_scalar_function <- function(in_type, out_type, fun) {
  fun <- rlang::as_function(fun)
  base_fun <- function(context, args) {
    args <- lapply(args, as.vector)
    result <- do.call(fun, args)
    as_arrow_array(result, type = context$output_type)
  }

  arrow_base_scalar_function(in_type, out_type, base_fun)
}

#' @rdname register_scalar_function
#' @export
arrow_base_scalar_function <- function(in_type, out_type, base_fun) {
  if (is.list(in_type)) {
    in_type <- lapply(in_type, as_scalar_function_in_type)
  } else {
    in_type <- list(as_scalar_function_in_type(in_type))
  }

  if (is.list(out_type)) {
    out_type <- lapply(out_type, as_scalar_function_out_type)
  } else {
    out_type <- list(as_scalar_function_out_type(out_type))
  }

  out_type <- rep_len(out_type, length(in_type))

  base_fun <- rlang::as_function(base_fun)
  if (length(formals(base_fun)) != 2) {
    abort("`base_fun` must accept exactly two arguments")
  }

  structure(
    base_fun,
    in_type = in_type,
    out_type = out_type,
    class = "arrow_base_scalar_function"
  )
}

as_scalar_function_in_type <- function(x) {
  if (inherits(x, "Field")) {
    schema(x)
  } else if (inherits(x, "DataType")) {
    schema(field("", x))
  } else {
    as_schema(x)
  }
}

as_scalar_function_out_type <- function(x) {
  if (is.function(x)) {
    x
  } else {
    x <- as_data_type(x)
    function(types) x
  }
}
