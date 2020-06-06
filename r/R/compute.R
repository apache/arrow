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

#' @export
sum.Array <- function(..., na.rm = FALSE) {
  args <- list(...)
  assert_that(length(args) == 1) # TODO: make chunked array if there are multiple arrays
  a <- ..1
  if (!na.rm && a$null_count > 0) {
    # Arrow sum function always drops NAs so handle that here
    Scalar$create(NA_integer_, type = a$type)
  } else {
    if (inherits(a$type, "Boolean")) {
      # Bool sum not implemented so cast to int
      a <- a$cast(int8())
    }
    shared_ptr(Scalar, call_function("sum", a))
  }
}

#' @export
sum.ChunkedArray <- sum.Array

#' @export
sum.Scalar <- sum.Array

#' @export
mean.Array <- function(..., na.rm = FALSE) {
  args <- list(...)
  assert_that(length(args) == 1) # TODO: make chunked array if there are multiple arrays
  a <- ..1
  if (!na.rm && a$null_count > 0) {
    # Arrow sum/mean function always drops NAs so handle that here
    Scalar$create(NA_integer_, type = a$type)
  } else {
    if (inherits(a$type, "Boolean")) {
      # Bool sum/mean not implemented so cast to int
      a <- a$cast(int8())
    }
    shared_ptr(Scalar, call_function("mean", a))
  }
}

#' @export
mean.ChunkedArray <- mean.Array

#' @export
mean.Scalar <- mean.Array
