<<<<<<< HEAD
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

=======
>>>>>>> Initial work for type metadata, with tests.
#' @include R6.R

`arrow::ArrayData` <- R6Class("arrow::ArrayData",
  inherit = `arrow::Object`,

  public = list(
    initialize = function(type, length, null_count = -1, offset = 0) {
      self$set_pointer(ArrayData_initialize(type, length, null_count, offset))
    }
  ),
  active = list(
    type = function() `arrow::DataType`$dispatch(ArrayData_get_type(self)),
    length = function() ArrayData_get_length(self),
    null_count = function() ArrayData_get_null_count(self),
    offset = function() ArrayData_get_offset(self)
  )
)

<<<<<<< HEAD
=======
#' @export
>>>>>>> Initial work for type metadata, with tests.
array_data <- function(...){
  `arrow::ArrayData`$new(...)
}

`arrow::Array` <- R6Class("arrow::Array",
  inherit = `arrow::Object`,
  public = list(
    initialize = function(x) {
      self$set_pointer(rvector_to_Array(x))
    },
    IsNull = function(i) Array_IsNull(self, i),
    IsValid = function(i) Array_IsValid(self, i),
    length = function() Array_length(self),
    offset = function() Array_offset(self),
    null_count = function() Array_null_count(self),
    type = function() `arrow::DataType`$dispatch(Array_type(self)),
    type_id = function() Array_type_id(self),
    Equals = function(other) Array_Equals(self, other),
    ApproxEquals = function(othet) Array_ApproxEquals(self, other),
    data = function() Array_data(self)
  )
)

#' @export
`length.arrow::Array` <- function(x) x$length()

#' @export
`==.arrow::Array` <- function(x, y) x$Equals(y)

#' @export
`!=.arrow::Array` <- function(x, y) !x$Equals(y)

<<<<<<< HEAD
=======
#' @export
>>>>>>> Initial work for type metadata, with tests.
MakeArray <- function(data){
  assert_that(inherits(data, "arrow::ArrayData"))
  `arrow::Array`$new(data)
}

#' @export
array <- function(...){
  `arrow::Array`$new(vctrs::vec_c(...))
}


