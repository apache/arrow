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

#' @title class arrow::Field
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname arrow__Field
#' @name arrow__Field
`arrow::Field` <- R6Class("arrow::Field", inherit = `arrow::Object`,
  public = list(
    ToString = function() {
      Field__ToString(self)
    },
    Equals = function(other) {
      inherits(other, "arrow::Field") && Field__Equals(self, other)
    }
  ),

  active = list(
    name = function() {
      Field__name(self)
    },
    nullable = function() {
      Field__nullable(self)
    },
    type = function() {
      `arrow::DataType`$dispatch(Field__type(self))
    }
  )
)

#' @export
`==.arrow::Field` <- function(lhs, rhs){
  lhs$Equals(rhs)
}

#' Factory for a `arrow::Field`
#'
#' @param name field name
#' @param type logical type, instance of `arrow::DataType`
#' @param metadata currently ignored
#'
#' @examples
#' field("x", int32())
#'
#' @export
field <- function(name, type, metadata) {
  assert_that(inherits(name, "character"), length(name) == 1L)
  assert_that(inherits(type, "arrow::DataType"))
  assert_that(missing(metadata), msg = "metadata= is currently ignored")
  shared_ptr(`arrow::Field`, Field__initialize(name, type))
}

.fields <- function(.list){
  assert_that( !is.null(nms <- names(.list)) )
  map2(nms, .list, field)
}
