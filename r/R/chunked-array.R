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
#' - `$length()`
#' - `$chunk(i)`
#' - `$as_vector()`
#' - `$Slice(offset, length = NULL)`
#' - `$cast(target_type, safe = TRUE, options = cast_options(safe))`
#' - `$null_count()`
#' - `$chunks()`
#' - `$num_chunks()`
#' - `$type()`
#' - `$View(type)`: Construct a zero-copy view of this chunked array with the given type.
#' - `$Validate()` : Perform any validation checks to determine obvious inconsistencies
#'    within the array's internal data. This can be an expensive check, potentially `O(length)`
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
        shared_ptr(ChunkedArray, ChunkedArray__Slice1(self, offset))
      } else {
        shared_ptr(ChunkedArray, ChunkedArray__Slice2(self, offset, length))
      }
    },
    Take = function(i) {
      if (self$num_chunks == 1) {
        # Call Array__Take on the 0th chunk and wrap that up
        ChunkedArray$create(self$chunk(0)$Take(i), type = self$type)
      } else {
        # 1) see if all i are in the same chunk, call Array__Take on that
        chunks <- self$chunks
        lengths <- map_int(chunks, length)
        offsets <- c(0L, cumsum(lengths[-1]))

        all_in <- which(min(i) >= offsets & max(i) <= (offsets + lengths - 1L))
        if (length(all_in)) {
          return(ChunkedArray$create(chunks[[all_in]]$Take(i - offsets[all_in]), type = self$type))
        }
        stop("ChunkedArray$Take() only supported when all i are in the same chunk")
        # 2) if identical(i, sort(i)), Take from each chunk appropriately?

        sorted_i <- sort(unique(i))
        # split sorted_i based on which chunk they fall in, then mapply to Take
        # from each Array in chunks.
        # Then concatenate the Arrays, and then do a Take on that to get back
        # to the original order? Then wrap as a ChunkedArray with a single array
      }
    },
    Filter = function(i) {
      if (is.logical(i)) {
        i <- Array$create(i)
      }
      shared_ptr(ChunkedArray, ChunkedArray__Filter(self, i))
    },
    cast = function(target_type, safe = TRUE, options = cast_options(safe)) {
      assert_is(target_type, "DataType")
      assert_is(options, "CastOptions")
      shared_ptr(ChunkedArray, ChunkedArray__cast(self, target_type, options))
    },
    View = function(type) {
      shared_ptr(ChunkedArray, ChunkedArray__View(self, type))
    },
    Validate = function() {
      ChunkedArray__Validate(self)
    },
    ToString = function() {
      out <- self$chunk(0)$ToString()
      if (self$num_chunks > 1) {
        # Regardless of whether the first array prints with ellipsis, we need
        # to ellipsize because there's more data than is contained in this
        # chunk
        if (grepl("...\n", out, fixed = TRUE)) {
          out <- sub("\\.\\.\\..*$", "...\n]", out)
        } else {
          out <- sub("\\n\\]$", ",\n  ...\n]", out)
        }
      }
      out
    },
    Equals = function(other) {
      ChunkedArray__Equals(self, other)
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

#' @export
length.ChunkedArray <- function(x) x$length()

#' @export
as.vector.ChunkedArray <- function(x, mode) x$as_vector()

#' @export
`[.ChunkedArray` <- filter_rows
