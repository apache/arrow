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

`arrow::ChunkedArray` <- R6Class("arrow::ChunkedArray", inherit = `arrow::Object`,
  public = list(
    length = function() ChunkedArray__length(self),
    null_count = function() ChunkedArray__null_count(self),
    num_chunks = function() ChunkedArray__num_chunks(self),
    chunk = function(i) shared_ptr(`arrow::Array`, ChunkedArray__chunk(self, i)),
    chunks = function() purrr::map(ChunkedArray__chunks(self), shared_ptr, class = `arrow::Array`),
    type = function() `arrow::DataType`$dispatch(ChunkedArray__type(self)),
    as_vector = function() ChunkedArray__as_vector(self),
    Slice = function(offset, length = NULL){
      if (is.null(length)) {
        shared_ptr(`arrow::ChunkedArray`, ChunkArray__Slice1(self, offset))
      } else {
        shared_ptr(`arrow::ChunkedArray`, ChunkArray__Slice2(self, offset, length))
      }
    }
  )
)

#' create an arrow::Array from an R vector
#'
#' @param \dots Vectors to coerce
#' @param type currently ignored
#'
#' @importFrom rlang list2
#' @export
chunked_array <- function(..., type){
  if (!missing(type)) {
    warn("The `type` argument is currently ignored")
  }
  shared_ptr(`arrow::ChunkedArray`, ChunkedArray__from_list(list2(...)))
}
