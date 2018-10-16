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

`arrow::DictionaryType` <- R6Class("arrow::DictionaryType",
  inherit = `arrow::FixedWidthType`,
  public = list(
    index_type = function() `arrow::DataType`$dispatch(DictionaryType__index_type(self)),
    name = function() DictionaryType__name(self),
    dictionary = function() `arrow::Array`$new(DictionaryType__dictionary(self)),
    ordered = function() DictionaryType__ordered(self)
  )

)

#' dictionary type factory
#'
#' @param type indices type, e.g. [int32()]
#' @param values values array, typically an arrow array of strings
#' @param ordered Is this an ordered dictionary
#'
#' @export
dictionary <- function(type, values, ordered = FALSE) {
  assert_that(
    inherits(type, "arrow::DataType"),
    inherits(values, "arrow::Array")
  )
  `arrow::DictionaryType`$new(DictionaryType__initialize(type, values, ordered))
}
