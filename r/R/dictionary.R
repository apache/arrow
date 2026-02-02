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

#' @title DictionaryType class
#'
#' @description
#' `DictionaryType` is a [FixedWidthType] that represents dictionary-encoded data.
#' Dictionary encoding stores unique values in a dictionary and uses integer-type
#' indices to reference them, which can be more memory-efficient for data with many
#' repeated values.
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section R6 Methods:
#'
#' - `$ToString()`: Return a string representation of the dictionary type
#' - `$code(namespace = FALSE)`: Return R code to create this dictionary type
#'
#' @section Active bindings:
#'
#' - `$index_type`: The [DataType] for the dictionary indices (must be an integer type,
#'   signed or unsigned)
#' - `$value_type`: The [DataType] for the dictionary values
#' - `$name`: The name of the type.
#' - `$ordered`: Whether the dictionary is ordered.
#'
#' @section Factory:
#'
#' `DictionaryType$create()` takes the following arguments:
#'
#' - `index_type`: A [DataType] for the indices (default [int32()])
#' - `value_type`: A [DataType] for the values (default [utf8()])
#' - `ordered`: Is this an ordered dictionary (default `FALSE`)?
#'
#' @rdname DictionaryType
#' @name DictionaryType
DictionaryType <- R6Class(
  "DictionaryType",
  inherit = FixedWidthType,
  public = list(
    ToString = function() {
      prettier_dictionary_type(DataType__ToString(self))
    },
    code = function(namespace = FALSE) {
      details <- list()
      if (self$index_type != int32()) {
        details$index_type <- self$index_type$code(namespace)
      }
      if (self$value_type != utf8()) {
        details$value_type <- self$value_type$code(namespace)
      }
      if (isTRUE(self$ordered)) {
        details$ordered <- TRUE
      }
      call2("dictionary", !!!details, .ns = if (namespace) "arrow")
    }
  ),
  active = list(
    index_type = function() DictionaryType__index_type(self),
    value_type = function() DictionaryType__value_type(self),
    name = function() DictionaryType__name(self),
    ordered = function() DictionaryType__ordered(self)
  )
)
DictionaryType$create <- function(index_type = int32(), value_type = utf8(), ordered = FALSE) {
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
