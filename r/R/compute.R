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

#' @include array.R
#' @include chunked-array.R
#' @include scalar.R

call_function <- function(function_name, ..., options = list()) {
  assert_that(is.string(function_name))
  compute__CallFunction(function_name, list(...), options)
}

#' @export
sum.Array <- function(..., na.rm = FALSE) scalar_aggregate("sum", ..., na.rm = na.rm)

#' @export
sum.ChunkedArray <- sum.Array

#' @export
sum.Scalar <- sum.Array

#' @export
mean.Array <- function(..., na.rm = FALSE) scalar_aggregate("mean", ..., na.rm = na.rm)

#' @export
mean.ChunkedArray <- mean.Array

#' @export
mean.Scalar <- mean.Array

#' @export
min.Array <- function(..., na.rm = FALSE) {
  scalar_aggregate("min_max", ..., na.rm = na.rm)$GetFieldByName("min")
}

#' @export
max.Array <- function(..., na.rm = FALSE) {
  scalar_aggregate("min_max", ..., na.rm = na.rm)$GetFieldByName("max")
}

scalar_aggregate <- function(FUN, ..., na.rm = FALSE) {
  a <- collect_arrays_from_dots(list(...))
  if (!na.rm && a$null_count > 0) {
    if (FUN %in% c("mean", "sum")) {
      # Arrow sum/mean function always drops NAs so handle that here
      # https://issues.apache.org/jira/browse/ARROW-9054
      return(Scalar$create(NA_real_))
    }
  }

  Scalar$create(call_function(FUN, a, options = list(na.rm = na.rm)))
}

collect_arrays_from_dots <- function(dots) {
  # Given a list that may contain both Arrays and ChunkedArrays,
  # return a single ChunkedArray containing all of those chunks
  # (may return a regular Array if there is only one element in dots)
  assert_that(all(map_lgl(dots, is.Array)))
  if (length(dots) == 1) {
    return(dots[[1]])
  }

  arrays <- unlist(lapply(dots, function(x) {
    if (inherits(x, "ChunkedArray")) {
      x$chunks
    } else {
      x
    }
  }))
  ChunkedArray$create(!!!arrays)
}

CastOptions <- R6Class("CastOptions", inherit = ArrowObject)

#' Cast options
#'
#' @param safe enforce safe conversion
#' @param allow_int_overflow allow int conversion, `!safe` by default
#' @param allow_time_truncate allow time truncate, `!safe` by default
#' @param allow_float_truncate allow float truncate, `!safe` by default
#'
#' @export
cast_options <- function(safe = TRUE,
                         allow_int_overflow = !safe,
                         allow_time_truncate = !safe,
                         allow_float_truncate = !safe) {
  shared_ptr(CastOptions,
    compute___CastOptions__initialize(allow_int_overflow, allow_time_truncate, allow_float_truncate)
  )
}
