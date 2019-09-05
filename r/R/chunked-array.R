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

#' @title class ChunkedArray
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname chunked-array
#' @name chunked-array
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
      assert_that(inherits(target_type, "DataType"))
      assert_that(inherits(options, "CastOptions"))
      shared_ptr(ChunkedArray, ChunkedArray__cast(self, target_type, options))
    }
  ),
  active = list(
    null_count = function() ChunkedArray__null_count(self),
    num_chunks = function() ChunkedArray__num_chunks(self),
    chunks = function() map(ChunkedArray__chunks(self), ~ Array$create(.x)),
    type = function() DataType$dispatch(ChunkedArray__type(self))
  )
)

ChunkedArray$create <- function(..., type = NULL) {
  shared_ptr(ChunkedArray, ChunkedArray__from_list(list2(...), type))
}

#' Create a [ChunkedArray][chunked-array] from various R vectors
#'
#' @param \dots Vectors to coerce
#' @param type currently ignored
#'
#' @export
chunked_array <- ChunkedArray$create
