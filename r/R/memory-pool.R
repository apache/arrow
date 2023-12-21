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

#' @include arrow-object.R
#'
#' @title MemoryPool class
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' - `backend_name`: one of "jemalloc", "mimalloc", or "system". Alternative
#'   memory allocators are optionally enabled at build time. Windows builds
#'   generally have `mimalloc`, and most others have both `jemalloc` (used by
#'   default) and `mimalloc`. To change memory allocators at runtime, set the
#'   environment variable `ARROW_DEFAULT_MEMORY_POOL` to one of those strings
#'   prior to loading the `arrow` library.
#' - `bytes_allocated`
#' - `max_memory`
#'
#' @rdname MemoryPool
#' @name MemoryPool
#' @keywords internal
MemoryPool <- R6Class("MemoryPool",
  inherit = ArrowObject,
  public = list(
    # TODO: Allocate
    # TODO: Reallocate
    # TODO: Free
  ),
  active = list(
    backend_name = function() MemoryPool__backend_name(self),
    bytes_allocated = function() MemoryPool__bytes_allocated(self),
    max_memory = function() MemoryPool__max_memory(self)
  )
)

#' Arrow's default [MemoryPool]
#'
#' @return the default [MemoryPool]
#' @export
#' @keywords internal
default_memory_pool <- function() {
  MemoryPool__default()
}
