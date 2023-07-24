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

#' @include arrow-object.R

# Base class for Array, ChunkedArray, and Scalar, for S3 method dispatch only.
# Does not exist in C++ class hierarchy
ArrowDatum <- R6Class("ArrowDatum",
  inherit = ArrowObject,
  public = list(
    cast = function(target_type, safe = TRUE, ...) {
      opts <- cast_options(safe, ...)
      opts$to_type <- as_type(target_type)
      call_function("cast", self, options = opts)
    },
    SortIndices = function(descending = FALSE) {
      assert_that(is.logical(descending))
      assert_that(length(descending) == 1L)
      assert_that(!is.na(descending))
      call_function(
        "sort_indices",
        self,
        options = list(names = "", orders = as.integer(descending))
      )
    }
  )
)

#' @export
length.ArrowDatum <- function(x) x$length()

#' @export
is.finite.ArrowDatum <- function(x) {
  is_fin <- call_function("is_finite", x)
  # for compatibility with base::is.finite(), return FALSE for NA_real_
  is_fin & !is.na(is_fin)
}

#' @export
is.infinite.ArrowDatum <- function(x) {
  is_inf <- call_function("is_inf", x)
  # for compatibility with base::is.infinite(), return FALSE for NA_real_
  is_inf & !is.na(is_inf)
}

#' @export
is.na.ArrowDatum <- function(x) {
  call_function("is_null", x, options = list(nan_is_null = TRUE))
}

#' @export
is.nan.ArrowDatum <- function(x) {
  if (x$type_id() %in% TYPES_WITH_NAN) {
    # TODO(ARROW-13366): if an option is added to the is_nan kernel to treat NA
    # as NaN, use that to simplify the code here
    call_function("is_nan", x) & call_function("is_valid", x)
  } else {
    Scalar$create(FALSE)$as_array(length(x))
  }
}

#' @export
as.vector.ArrowDatum <- function(x, mode) {
  x$as_vector()
}

#' @export
Ops.ArrowDatum <- function(e1, e2) {
  if (missing(e2)) {
    switch(.Generic,
      "!" = return(eval_array_expression(.Generic, e1)),
      "+" = return(eval_array_expression(.Generic, 0L, e1)),
      "-" = return(eval_array_expression("negate_checked", e1)),
    )
  }

  switch(.Generic,
    "+" = ,
    "-" = ,
    "*" = ,
    "/" = ,
    "^" = ,
    "%%" = ,
    "%/%" = ,
    "==" = ,
    "!=" = ,
    "<" = ,
    "<=" = ,
    ">=" = ,
    ">" = ,
    "&" = ,
    "|" = {
      eval_array_expression(.Generic, e1, e2)
    },
    stop(paste0("Unsupported operation on `", class(e1)[1L], "` : "), .Generic, call. = FALSE)
  )
}

#' @export
Math.ArrowDatum <- function(x, ..., base = exp(1), digits = 0) {
  switch(.Generic,
    abs = eval_array_expression("abs_checked", x),
    ceiling = eval_array_expression("ceil", x),
    sign = ,
    floor = ,
    trunc = ,
    acos = ,
    asin = ,
    atan = ,
    cos = ,
    sin = ,
    tan = {
      eval_array_expression(.Generic, x)
    },
    log = eval_array_expression("logb_checked", x, base),
    log10 = eval_array_expression("log10_checked", x),
    round = eval_array_expression(
      "round",
      x,
      options = list(ndigits = digits, round_mode = RoundMode$HALF_TO_EVEN)
    ),
    sqrt = eval_array_expression("sqrt_checked", x),
    exp = eval_array_expression("power_checked", exp(1), x),
    signif = ,
    expm1 = ,
    log1p = ,
    cospi = ,
    sinpi = ,
    tanpi = ,
    cosh = ,
    sinh = ,
    tanh = ,
    acosh = ,
    asinh = ,
    atanh = ,
    lgamma = ,
    gamma = ,
    digamma = ,
    trigamma = ,
    cumsum = eval_array_expression("cumulative_sum_checked", x),
    cumprod = ,
    cummax = ,
    cummin = ,
    stop(paste0("Unsupported operation on `", class(x)[1L], "` : "), .Generic, call. = FALSE)
  )
}

# Wrapper around call_function that:
# (1) maps R function names to Arrow C++ compute ("/" --> "divide_checked")
# (2) wraps R input args as Array or Scalar
eval_array_expression <- function(FUN,
                                  ...,
                                  args = list(...),
                                  options = empty_named_list()) {
  if (FUN == "-" && length(args) == 1L) {
    if (inherits(args[[1]], "ArrowObject")) {
      return(eval_array_expression("negate_checked", args[[1]]))
    } else {
      return(-args[[1]])
    }
  }
  args <- lapply(args, .wrap_arrow)

  # In Arrow, "divide" is one function, which does integer division on
  # integer inputs and floating-point division on floats
  if (FUN == "/") {
    # TODO: omg so many ways it's wrong to assume these types
    args <- map(args, ~ .$cast(float64()))
  } else if (FUN == "%/%") {
    # In R, integer division works like floor(float division)
    out <- eval_array_expression("/", args = args)

    # integer output only for all integer input
    int_type_ids <- Type[toupper(INTEGER_TYPES)]
    numerator_is_int <- args[[1]]$type_id() %in% int_type_ids
    denominator_is_int <- args[[2]]$type_id() %in% int_type_ids

    if (numerator_is_int && denominator_is_int) {
      out_float <- eval_array_expression(
        "if_else",
        eval_array_expression("equal", args[[2]], 0L),
        Scalar$create(NA_integer_),
        eval_array_expression("floor", out)
      )
      return(out_float$cast(args[[1]]$type))
    } else {
      return(eval_array_expression("floor", out))
    }
  } else if (FUN == "%%") {
    # We can't simply do {e1 - e2 * ( e1 %/% e2 )} since Ops.Array evaluates
    # eagerly, but we can build that up
    quotient <- eval_array_expression("%/%", args = args)
    base <- eval_array_expression("*", quotient, args[[2]])
    # this cast is to ensure that the result of this and e1 are the same
    # (autocasting only applies to scalars)
    base <- base$cast(args[[1]]$type)
    return(eval_array_expression("-", args[[1]], base))
  }

  call_function(
    .array_function_map[[FUN]] %||% FUN,
    args = args,
    options = options
  )
}

.wrap_arrow <- function(arg) {
  if (!inherits(arg, "ArrowObject")) {
    arg <- Scalar$create(arg)
  }
  arg
}

#' @export
na.omit.ArrowDatum <- function(object, ...) {
  object$Filter(!is.na(object))
}

#' @export
na.exclude.ArrowDatum <- na.omit.ArrowDatum

#' @export
na.fail.ArrowDatum <- function(object, ...) {
  if (object$null_count > 0) {
    stop("missing values in object", call. = FALSE)
  }
  object
}

filter_rows <- function(x, i, keep_na = TRUE, ...) {
  # General purpose function for [ row subsetting with R semantics
  # Based on the input for `i`, calls x$Filter, x$Slice, or x$Take
  nrows <- x$num_rows %||% x$length() # Depends on whether Array or Table-like
  if (is.logical(i)) {
    if (isTRUE(i)) {
      # Shortcut without doing any work
      x
    } else {
      i <- rep_len(i, nrows) # For R recycling behavior; consider vctrs::vec_recycle()
      x$Filter(i, keep_na)
    }
  } else if (is.numeric(i)) {
    if (all(i < 0)) {
      # in R, negative i means "everything but i"
      i <- setdiff(seq_len(nrows), -1 * i)
    }
    if (is.sliceable(i)) {
      x$Slice(i[1] - 1, length(i))
    } else if (all(i > 0)) {
      x$Take(i - 1)
    } else {
      stop("Cannot mix positive and negative indices", call. = FALSE)
    }
  } else if (is.Array(i, INTEGER_TYPES)) {
    # NOTE: this doesn't do the - 1 offset
    x$Take(i)
  } else if (is.Array(i, "bool")) {
    x$Filter(i, keep_na)
  } else {
    # Unsupported cases
    if (is.Array(i)) {
      stop("Cannot extract rows with an Array of type ", i$type$ToString(), call. = FALSE)
    }
    stop("Cannot extract rows with an object of class ", class(i), call. = FALSE)
  }
}

#' @export
`[.ArrowDatum` <- filter_rows

#' @importFrom utils head
#' @export
head.ArrowDatum <- function(x, n = 6L, ...) {
  assert_is(n, c("numeric", "integer"))
  assert_that(length(n) == 1)
  len <- NROW(x)
  if (n < 0) {
    # head(x, negative) means all but the last n rows
    n <- max(len + n, 0)
  } else {
    n <- min(len, n)
  }
  if (!is.integer(n)) {
    n <- floor(n)
  }
  if (n == len) {
    return(x)
  }
  x$Slice(0, n)
}

#' @importFrom utils tail
#' @export
tail.ArrowDatum <- function(x, n = 6L, ...) {
  assert_is(n, c("numeric", "integer"))
  assert_that(length(n) == 1)
  if (!is.integer(n)) {
    n <- floor(n)
  }
  len <- NROW(x)
  if (n < 0) {
    # tail(x, negative) means all but the first n rows
    n <- min(-n, len)
  } else {
    n <- max(len - n, 0)
  }
  if (n == 0) {
    return(x)
  }
  x$Slice(n)
}

is.sliceable <- function(i) {
  # Determine whether `i` can be expressed as a $Slice() command
  is.numeric(i) &&
    length(i) > 0 &&
    all(i > 0) &&
    i[1] <= i[length(i)] &&
    identical(as.integer(i), i[1]:i[length(i)])
}

#' @export
as.double.ArrowDatum <- function(x, ...) as.double(as.vector(x), ...)

#' @export
as.integer.ArrowDatum <- function(x, ...) as.integer(as.vector(x), ...)

#' @export
as.character.ArrowDatum <- function(x, ...) as.character(as.vector(x), ...)

#' @export
sort.ArrowDatum <- function(x, decreasing = FALSE, na.last = NA, ...) {
  # Arrow always sorts nulls at the end of the array. This corresponds to
  # sort(na.last = TRUE). For the other two cases (na.last = NA and
  # na.last = FALSE) we need to use workarounds.
  # TODO(ARROW-14085): use NullPlacement ArraySortOptions instead of this workaround
  if (is.na(na.last)) {
    # Filter out NAs before sorting
    x <- x$Filter(!is.na(x))
    x$Take(x$SortIndices(descending = decreasing))
  } else if (na.last) {
    x$Take(x$SortIndices(descending = decreasing))
  } else {
    # Create a new array that encodes missing values as 1 and non-missing values
    # as 0. Sort descending by that array first to get the NAs at the beginning
    tbl <- Table$create(x = x, `is_na` = as.integer(is.na(x)))
    tbl$x$Take(tbl$SortIndices(names = c("is_na", "x"), descending = c(TRUE, decreasing)))
  }
}
