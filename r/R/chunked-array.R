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
#' primitive Arrow [Arrays][Array] logically as one large array.
#' @section Factory:
#' The `ChunkedArray$create()` factory method instantiates the object from
#' various Arrays or R vectors. `chunked_array()` is an alias for it.
#'
#' @section Methods:
#'
#' - `$length()`
#' - `$chunk(i)`
#' - `$as_vector()`
#' - `$Slice(offset, length = NULL)`
#' - `$cast(target_type, safe = TRUE, options = cast_options(safe))`
#' - `$null_count()`
#' - `$chunks()`
#' - `$num_chunks()`
#' - `$type()`
#'
#' @rdname ChunkedArray
#' @name ChunkedArray
#' @seealso [Array]
#' @export
ChunkedArray <- R6Class("ChunkedArray", inherit = Object,
  public = list(
    length = function() ChunkedArray__length(self),
    chunk = function(i) Array$create(ChunkedArray__chunk(self, i)),
    as_vector = function() ChunkedArray__as_vector(self),
    Slice = function(offset, length = NULL){
      if (is.null(length)) {
        shared_ptr(ChunkedArray, ChunkArray__Slice1(self, offset))
      } else {
        shared_ptr(ChunkedArray, ChunkArray__Slice2(self, offset, length))
      }
    },
    cast = function(target_type, safe = TRUE, options = cast_options(safe)) {
      assert_is(target_type, "DataType")
      assert_is(options, "CastOptions")
      shared_ptr(ChunkedArray, ChunkedArray__cast(self, target_type, options))
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
  shared_ptr(ChunkedArray, ChunkedArray__from_list(list2(...), type))
}

#' @param \dots Vectors to coerce
#' @param type currently ignored
#' @rdname ChunkedArray
#' @export
chunked_array <- ChunkedArray$create
