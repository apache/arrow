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
#' @title Field class
#' @usage NULL
#' @format NULL
#' @docType class
#' @description `field()` lets you create an `arrow::Field` that maps a
#' [DataType][data-type] to a column name. Fields are contained in
#' [Schemas][Schema].
#' @section Methods:
#'
#' - `f$ToString()`: convert to a string
#' - `f$Equals(other)`: test for equality. More naturally called as `f == other`
#'
#' @name Field
#' @rdname Field-class
#' @export
Field <- R6Class("Field",
  inherit = ArrowObject,
  public = list(
    ToString = function() {
      prettier_dictionary_type(Field__ToString(self))
    },
    Equals = function(other, ...) {
      inherits(other, "Field") && Field__Equals(self, other)
    },
    export_to_c = function(ptr) ExportField(self, ptr)
  ),
  active = list(
    name = function() {
      Field__name(self)
    },
    nullable = function() {
      Field__nullable(self)
    },
    type = function() {
      Field__type(self)
    }
  )
)
Field$create <- function(name, type, metadata, nullable = TRUE) {
  assert_that(inherits(name, "character"), length(name) == 1L)
  type <- as_type(type, name)
  assert_that(missing(metadata), msg = "metadata= is currently ignored")
  Field__initialize(enc2utf8(name), type, nullable)
}
#' @include arrowExports.R
Field$import_from_c <- ImportField

#' Create a Field
#'
#' @param name field name
#' @param type logical type, instance of [DataType]
#' @param metadata currently ignored
#' @param nullable TRUE if field is nullable
#'
#' @examples
#' field("x", int32())
#' @rdname Field
#' @seealso [Field]
#' @export
field <- Field$create

.fields <- function(.list, nullable = TRUE) {
  if (length(.list)) {
    assert_that(!is.null(nms <- names(.list)))
    map2(nms, .list, field)
  } else {
    list()
  }
}
