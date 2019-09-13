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

#' @title Array class
#' @description Array base type. Immutable data array with some logical type
#' and some length.
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Factory:
#' The `Array$create()` factory method instantiates an `Array` and
#' takes the following arguments:
#' * `x`: an R vector, list, or `data.frame`
#' * `type`: an optional [data type][data-type] for `x`. If omitted, the type
#'    will be inferred from the data.
#' @section Usage:
#'
#' ```
#' a <- Array$create(x)
#' length(a)
#'
#' print(a)
#' a == a
#' ```
#'
#' @section Methods:
#'
#' - `$IsNull(i)`: Return true if value at index is null. Does not boundscheck
#' - `$IsValid(i)`: Return true if value at index is valid. Does not boundscheck
#' - `$length()`: Size in the number of elements this array contains
#' - `$offset()`: A relative position into another array's data, to enable zero-copy slicing
#' - `$null_count()`: The number of null entries in the array
#' - `$type()`: logical type of data
#' - `$type_id()`: type id
#' - `$Equals(other)` : is this array equal to `other`
#' - `$ApproxEquals(other)` :
#' - `$data()`: return the underlying [ArrayData][ArrayData]
#' - `$as_vector()`: convert to an R vector
#' - `$ToString()`: string representation of the array
#' - `$Slice(offset, length = NULL)` : Construct a zero-copy slice of the array with the indicated offset and length. If length is `NULL`, the slice goes until the end of the array.
#' - `$RangeEquals(other, start_idx, end_idx, other_start_idx)` :
#'
#' @rdname array
#' @name array
#' @export
Array <- R6Class("Array",
  inherit = Object,
  public = list(
    IsNull = function(i) Array__IsNull(self, i),
    IsValid = function(i) Array__IsValid(self, i),
    length = function() Array__length(self),
    type_id = function() Array__type_id(self),
    Equals = function(other) Array__Equals(self, other),
    ApproxEquals = function(other) Array__ApproxEquals(self, other),
    data = function() shared_ptr(ArrayData, Array__data(self)),
    as_vector = function() Array__as_vector(self),
    ToString = function() Array__ToString(self),
    Slice = function(offset, length = NULL){
      if (is.null(length)) {
        shared_ptr(Array, Array__Slice1(self, offset))
      } else {
        shared_ptr(Array, Array__Slice2(self, offset, length))
      }
    },
    RangeEquals = function(other, start_idx, end_idx, other_start_idx) {
      assert_is(other, "Array")
      Array__RangeEquals(self, other, start_idx, end_idx, other_start_idx)
    },
    cast = function(target_type, safe = TRUE, options = cast_options(safe)) {
      assert_is(target_type, "DataType")
      assert_is(options, "CastOptions")
      Array$create(Array__cast(self, target_type, options))
    }
  ),
  active = list(
    null_count = function() Array__null_count(self),
    offset = function() Array__offset(self),
    type = function() DataType$create(Array__type(self))
  )
)

DictionaryArray <- R6Class("DictionaryArray", inherit = Array,
  public = list(
    indices = function() Array$create(DictionaryArray__indices(self)),
    dictionary = function() Array$create(DictionaryArray__dictionary(self))
  )
)

StructArray <- R6Class("StructArray", inherit = Array,
  public = list(
    field = function(i) Array$create(StructArray__field(self, i)),
    GetFieldByName = function(name) Array$create(StructArray__GetFieldByName(self, name)),
    Flatten = function() map(StructArray__Flatten(self), ~ Array$create(.x))
  )
)

ListArray <- R6Class("ListArray", inherit = Array,
  public = list(
    values = function() Array$create(ListArray__values(self)),
    value_length = function(i) ListArray__value_length(self, i),
    value_offset = function(i) ListArray__value_offset(self, i),
    raw_value_offsets = function() ListArray__raw_value_offsets(self)
  ),
  active = list(
    value_type = function() DataType$create(ListArray__value_type(self))
  )
)

# Add a class method
Array$create <- function(x, type = NULL) {
  if (!inherits(x, "externalptr")) {
    x <- Array__from_vector(x, type)
  }
  a <- shared_ptr(Array, x)
  if (a$type_id() == Type$DICTIONARY){
    a <- shared_ptr(DictionaryArray, x)
  } else if (a$type_id() == Type$STRUCT) {
    a <- shared_ptr(StructArray, x)
  } else if (a$type_id() == Type$LIST) {
    a <- shared_ptr(ListArray, x)
  }
  a
}

#' @export
length.Array <- function(x) x$length()

#' @export
`==.Array` <- function(x, y) x$Equals(y)
