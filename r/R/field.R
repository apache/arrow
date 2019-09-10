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

#' @include arrow-package.R
#' @title class arrow::Field
#' @docType class
#' @description `field()` lets you create an `arrow::Field` that maps a
#' [DataType][data-type] to a column name. Fields are contained in
#' [Schemas][Schema].
#' @section Methods:
#'
#' - `f$ToString()`: convert to a string
#' - `f$Equals(other)`: test for equality. More naturally called as `f == other`
#'
#' @rdname Field
#' @name Field
#' @export
Field <- R6Class("Field", inherit = Object,
  public = list(
    ToString = function() {
      Field__ToString(self)
    },
    Equals = function(other) {
      inherits(other, "Field") && Field__Equals(self, other)
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
      DataType$create(Field__type(self))
    }
  )
)
Field$create <- function(name, type, metadata) {
  assert_that(inherits(name, "character"), length(name) == 1L)
  if (!inherits(type, "DataType")) {
    if (identical(type, double())) {
      # Magic so that we don't have to mask this base function
      type <- float64()
    } else {
      stop(name, " must be arrow::DataType, not ", class(type), call. = FALSE)
    }
  }
  assert_that(missing(metadata), msg = "metadata= is currently ignored")
  shared_ptr(Field, Field__initialize(name, type, TRUE))
}

#' @export
`==.Field` <- function(lhs, rhs){
  lhs$Equals(rhs)
}

#' @param name field name
#' @param type logical type, instance of [DataType]
#' @param metadata currently ignored
#'
#' @examples
#' \donttest{
#' field("x", int32())
#' }
#' @rdname Field
#' @export
field <- Field$create

.fields <- function(.list){
  assert_that(!is.null(nms <- names(.list)))
  map2(nms, .list, field)
}
