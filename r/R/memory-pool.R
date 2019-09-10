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
#'
#' @title class arrow::MemoryPool
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname MemoryPool
#' @name MemoryPool
MemoryPool <- R6Class("MemoryPool",
  inherit = Object,
  public = list(
    # TODO: Allocate
    # TODO: Reallocate
    # TODO: Free
    bytes_allocated = function() MemoryPool__bytes_allocated(self),
    max_memory = function() MemoryPool__max_memory(self)
  )
)

#' default [arrow::MemoryPool][MemoryPool]
#'
#' @return the default [arrow::MemoryPool][MemoryPool]
#' @export
default_memory_pool <- function() {
  shared_ptr(MemoryPool, MemoryPool__default())
}
