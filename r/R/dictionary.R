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

#' @include type.R

#' @title class DictionaryType
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname DictionaryType
#' @name DictionaryType
DictionaryType <- R6Class("DictionaryType",
  inherit = FixedWidthType,
  public = list(
    ToString = function() {
      prettier_dictionary_type(DataType__ToString(self))
    },
    code = function() {
      details <- list()
      if (self$index_type != int32()) {
        details$index_type <- self$index_type$code()
      }
      if (self$value_type != utf8()) {
        details$value_type <- self$value_type$code()
      }
      if (isTRUE(self$ordered)) {
        details$ordered <- TRUE
      }
      call2("dictionary", !!!details)
    }
  ),
  active = list(
    index_type = function() DictionaryType__index_type(self),
    value_type = function() DictionaryType__value_type(self),
    name = function() DictionaryType__name(self),
    ordered = function() DictionaryType__ordered(self)
  )
)
DictionaryType$create <- function(index_type = int32(),
                                  value_type = utf8(),
                                  ordered = FALSE) {
  assert_is(index_type, "DataType")
  assert_is(value_type, "DataType")
  DictionaryType__initialize(index_type, value_type, ordered)
}

#' Create a dictionary type
#'
#' @param index_type A DataType for the indices (default [int32()])
#' @param value_type A DataType for the values (default [utf8()])
#' @param ordered Is this an ordered dictionary (default `FALSE`)?
#'
#' @return A [DictionaryType]
#' @seealso [Other Arrow data types][data-type]
#' @export
dictionary <- DictionaryType$create

prettier_dictionary_type <- function(x) {
  # Prettier format the "ordered" attribute
  x <- sub(", ordered=0", "", x)
  sub("ordered=1", "ordered", x)
}
