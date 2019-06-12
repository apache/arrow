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

#' @title class arrow::DictionaryType
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname arrow__DictionaryType
#' @name arrow__DictionaryType
`arrow::DictionaryType` <- R6Class("arrow::DictionaryType",
  inherit = `arrow::FixedWidthType`,

  active = list(
    index_type = function() `arrow::DataType`$dispatch(DictionaryType__index_type(self)),
    value_type = function() `arrow::DataType`$dispatch(DictionaryType__value_type(self)),
    name = function() DictionaryType__name(self),
    ordered = function() DictionaryType__ordered(self)
  )
)

#' dictionary type factory
#'
#' @param index_type index type, e.g. [int32()]
#' @param value_type value type, probably [utf8()]
#' @param ordered Is this an ordered dictionary ?
#'
#' @return a [arrow::DictionaryType][arrow__DictionaryType]
#'
#' @export
dictionary <- function(index_type, value_type, ordered = FALSE) {
  assert_that(
    inherits(index_type, "arrow::DataType"),
    inherits(index_type, "arrow::DataType")
  )
  shared_ptr(`arrow::DictionaryType`, DictionaryType__initialize(index_type, value_type, ordered))
}
