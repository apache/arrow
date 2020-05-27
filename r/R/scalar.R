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

#' @include arrowExports.R

#' Arrow scalars
#'
#' @description
#' `Scalar`s are used to store a singular value of an arrow `DataType`
#'
#' @name Scalar
#' @rdname Scalar
#' @export
Scalar <- R6Class("Scalar", inherit = ArrowObject,
  public = list(
    ToString = function() Scalar__ToString(self),
    as_vector = function() Scalar__as_vector(self)
  ),
  active = list(
    is_valid = function() Scalar__is_valid(self),
    type = function() DataType$create(Scalar__type(self))
  )
)
Scalar$create <- function(x) {
  # TODO: it would probably be best if an explicit type could be provided
  if (!inherits(x, "externalptr")) {
    x <- Scalar__create(x)
  }
  shared_ptr(Scalar, x)
}

#' @export
length.Scalar <- function(x) 1

#' @export
is.na.Scalar <- function(x) !x$is_valid

#' @export
as.vector.Scalar <- function(x, mode) x$as_vector()
