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

#' @title ChunkedArray class
#' @usage NULL
#' @format NULL
#' @docType class
#' @description A `ChunkedArray` is a data structure managing a list of
#' primitive Arrow [Arrays][Array] logically as one large array. Chunked arrays
#' may be grouped together in a [Table].
#' @section Factory:
#' The `ChunkedArray$create()` factory method instantiates the object from
#' various Arrays or R vectors. `chunked_array()` is an alias for it.
#'
#' @section Methods:
#'
#' - `$length()`: Size in the number of elements this array contains
#' - `$chunk(i)`: Extract an `Array` chunk by integer position
#' - `$as_vector()`: convert to an R vector
#' - `$Slice(offset, length = NULL)`: Construct a zero-copy slice of the array
#'    with the indicated offset and length. If length is `NULL`, the slice goes
#'    until the end of the array.
#' - `$Take(i)`: return a `ChunkedArray` with values at positions given by
#'    integers `i`. If `i` is an Arrow `Array` or `ChunkedArray`, it will be
#'    coerced to an R vector before taking.
#' - `$Filter(i, keep_na = TRUE)`: return a `ChunkedArray` with values at positions where
#'    logical vector or Arrow boolean-type `(Chunked)Array` `i` is `TRUE`.
#' - `$cast(target_type, safe = TRUE, options = cast_options(safe))`: Alter the
#'    data in the array to change its type.
#' - `$null_count()`: The number of null entries in the array
#' - `$chunks()`: return a list of `Array`s
#' - `$num_chunks()`: integer number of chunks in the `ChunkedArray`
#' - `$type()`: logical type of data
#' - `$View(type)`: Construct a zero-copy view of this `ChunkedArray` with the
#'    given type.
#' - `$Validate()`: Perform any validation checks to determine obvious inconsistencies
#'    within the array's internal data. This can be an expensive check, potentially `O(length)`
#'
#' @rdname ChunkedArray
#' @name ChunkedArray
#' @seealso [Array]
#' @export
ChunkedArray <- R6Class("ChunkedArray", inherit = ArrowObject,
  public = list(
    length = function() ChunkedArray__length(self),
    chunk = function(i) Array$create(ChunkedArray__chunk(self, i)),
    as_vector = function() ChunkedArray__as_vector(self),
    Slice = function(offset, length = NULL){
      if (is.null(length)) {
        shared_ptr(ChunkedArray, ChunkedArray__Slice1(self, offset))
      } else {
        shared_ptr(ChunkedArray, ChunkedArray__Slice2(self, offset, length))
      }
    },
    Take = function(i) {
      if (is.numeric(i)) {
        i <- as.integer(i)
      }
      if (is.integer(i)) {
        i <- Array$create(i)
      }
      shared_ptr(ChunkedArray, call_function("take", self, i))
    },
    Filter = function(i, keep_na = TRUE) {
      if (is.logical(i)) {
        i <- Array$create(i)
      }
      shared_ptr(ChunkedArray, call_function("filter", self, i, options = list(keep_na = keep_na)))
    },
    cast = function(target_type, safe = TRUE, options = cast_options(safe)) {
      assert_is(options, "CastOptions")
      shared_ptr(ChunkedArray, ChunkedArray__cast(self, as_type(target_type), options))
    },
    View = function(type) {
      shared_ptr(ChunkedArray, ChunkedArray__View(self, as_type(type)))
    },
    Validate = function() {
      ChunkedArray__Validate(self)
    },
    ToString = function() {
      ChunkedArray__ToString(self)
    },
    Equals = function(other, ...) {
      inherits(other, "ChunkedArray") && ChunkedArray__Equals(self, other)
    }
  ),
  active = list(
    null_count = function() ChunkedArray__null_count(self),
    num_chunks = function() ChunkedArray__num_chunks(self),
    chunks = function() map(ChunkedArray__chunks(self), Array$create),
    type = function() DataType$create(ChunkedArray__type(self))
  )
)

ChunkedArray$create <- function(..., type = NULL) {
  if (!is.null(type)) {
    type <- as_type(type)
  }
  shared_ptr(ChunkedArray, ChunkedArray__from_list(list2(...), type))
}

#' @param \dots Vectors to coerce
#' @param type currently ignored
#' @rdname ChunkedArray
#' @export
chunked_array <- ChunkedArray$create

#' @export
length.ChunkedArray <- function(x) x$length()

#' @export
as.vector.ChunkedArray <- function(x, mode) x$as_vector()

#' @export
is.na.ChunkedArray <- function(x) unlist(lapply(x$chunks, is.na))

#' @export
`[.ChunkedArray` <- filter_rows

#' @export
head.ChunkedArray <- head.Array

#' @export
tail.ChunkedArray <- tail.Array

#' @export
as.double.ChunkedArray <- as.double.Array

#' @export
as.integer.ChunkedArray <- as.integer.Array

#' @export
as.character.ChunkedArray <- as.character.Array
