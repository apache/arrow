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

`arrow::Array` <- R6Class("arrow::Array",
  inherit = `arrow::Object`,
  public = list(
    IsNull = function(i) Array__IsNull(self, i),
    IsValid = function(i) Array__IsValid(self, i),
    length = function() Array__length(self),
    offset = function() Array__offset(self),
    null_count = function() Array__null_count(self),
    type = function() `arrow::DataType`$dispatch(Array__type(self)),
    type_id = function() Array__type_id(self),
    Equals = function(other) Array__Equals(self, other),
    ApproxEquals = function(othet) Array__ApproxEquals(self, other),
    data = function() shared_ptr(`arrow::ArrayData`, Array__data(self)),
    as_vector = function() Array__as_vector(self),
    ToString = function() Array__ToString(self),
    Slice = function(offset, length = NULL){
      if (is.null(length)) {
        shared_ptr(`arrow::Array`, Array__Slice1(self, offset))
      } else {
        shared_ptr(`arrow::Array`, Array__Slice2(self, offset, length))
      }
    },
    RangeEquals = function(other, start_idx, end_idx, other_start_idx) {
      assert_that(inherits(other, "arrow::Array"))
      Array__RangeEquals(self, other, start_idx, end_idx, other_start_idx)
    }
  )
)

`arrow::Array`$dispatch <- function(xp){
  a <- shared_ptr(`arrow::Array`, xp)
  if(a$type_id() == Type$DICTIONARY){
    a <- shared_ptr(`arrow::DictionaryArray`, xp)
  }
  a
}

#' @export
`length.arrow::Array` <- function(x) x$length()

#' @export
`==.arrow::Array` <- function(x, y) x$Equals(y)

#' create an arrow::Array from an R vector
#'
#' @param \dots Vectors to coerce
#' @param type currently ignored
#'
#' @importFrom rlang warn
#' @export
array <- function(..., type){
  if (!missing(type)) {
    warn("The `type` argument is currently ignored")
  }
  `arrow::Array`$dispatch(Array__from_vector(vctrs::vec_c(...)))
}

`arrow::DictionaryArray` <- R6Class("arrow::DictionaryArray", inherit = `arrow::Array`,
  public = list(
    indices = function() `arrow::Array`$dispatch(DictionaryArray__indices(self)),
    dictionary = function() `arrow::Array`$dispatch(DictionaryArray__dictionary(self))
  )
)

