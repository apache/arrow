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

#' @title class arrow::ChunkedArray
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname arrow__ChunkedArray
#' @name arrow__ChunkedArray
`arrow::ChunkedArray` <- R6Class("arrow::ChunkedArray", inherit = `arrow::Object`,
  public = list(
    length = function() ChunkedArray__length(self),
    chunk = function(i) `arrow::Array`$dispatch(ChunkedArray__chunk(self, i)),
    as_vector = function() ChunkedArray__as_vector(self),
    Slice = function(offset, length = NULL){
      if (is.null(length)) {
        shared_ptr(`arrow::ChunkedArray`, ChunkArray__Slice1(self, offset))
      } else {
        shared_ptr(`arrow::ChunkedArray`, ChunkArray__Slice2(self, offset, length))
      }
    },
    cast = function(target_type, safe = TRUE, options = cast_options(safe)) {
      assert_that(inherits(target_type, "arrow::DataType"))
      assert_that(inherits(options, "arrow::compute::CastOptions"))
      shared_ptr(`arrow::ChunkedArray`, ChunkedArray__cast(self, target_type, options))
    }
  ),
  active = list(
    null_count = function() ChunkedArray__null_count(self),
    num_chunks = function() ChunkedArray__num_chunks(self),
    chunks = function() map(ChunkedArray__chunks(self), ~ `arrow::Array`$dispatch(.x)),
    type = function() `arrow::DataType`$dispatch(ChunkedArray__type(self))
  )
)

#' create an [arrow::ChunkedArray][arrow__ChunkedArray] from various R vectors
#'
#' @param \dots Vectors to coerce
#' @param type currently ignored
#'
#' @export
chunked_array <- function(..., type = NULL){
  shared_ptr(`arrow::ChunkedArray`, ChunkedArray__from_list(list2(...), type))
}
