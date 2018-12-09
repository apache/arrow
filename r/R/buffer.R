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

#' @include R6.R
#' @include enums.R

#' @title class arrow::Buffer
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' - `$is_mutable()` :
#' - `$ZeroPadding()` :
#' - `$size()` :
#' - `$capacity()`:
#'
#' @rdname arrow__Buffer
#' @name arrow__Buffer
`arrow::Buffer` <- R6Class("arrow::Buffer", inherit = `arrow::Object`,
  public = list(
    ZeroPadding = function() Buffer__ZeroPadding(self)
  ),

  active = list(
    is_mutable = function() Buffer__is_mutable(self),
    size = function() Buffer__size(self),
    capacity = function() Buffer__capacity(self)
  )
)

#' Create a [arrow::Buffer][arrow__Buffer] from an R object
#'
#' @param x R object. Only raw, numeric and integer vectors are currently supported
#'
#' @return an instance of [arrow::Buffer][arrow__Buffer] that borrows memory from `x`
#'
#' @export
buffer <- function(x){
  UseMethod("buffer")
}

#' @export
buffer.default <- function(x) {
  stop("cannot convert to Buffer")
}

#' @export
buffer.raw <- function(x) {
  shared_ptr(`arrow::Buffer`, r___RBuffer__initialize(x))
}

#' @export
buffer.numeric <- function(x) {
  shared_ptr(`arrow::Buffer`, r___RBuffer__initialize(x))
}

#' @export
buffer.integer <- function(x) {
  shared_ptr(`arrow::Buffer`, r___RBuffer__initialize(x))
}

#' @export
buffer.complex <- function(x) {
  shared_ptr(`arrow::Buffer`, r___RBuffer__initialize(x))
}

