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
#' @include enums.R
#' @include buffer.R

`arrow::io::RandomAccessFile` <- R6Class("arrow::io::RandomAccessFile", inherit = `arrow::Object`,
  public = list(
    GetSize = function() io___RandomAccessFile__GetSize(self),
    supports_zero_copy = function() io___RandomAccessFile__supports_zero_copy(self)
  )
)

`arrow::io::MemoryMappedFile` <- R6Class("arrow::io::MemoryMappedFile", inherit = `arrow::io::RandomAccessFile`,
  public = list(
    Close = function() io___MemoryMappedFile__Close(self),
    Tell = function() io___MemoryMappedFile__Tell(self),
    Seek = function(position) io___MemoryMappedFile__Seek(self, position),
    Resize = function(size) io___MemoryMappedFile__Resize(self, size),
    Read = function(nbytes) `arrow::Buffer`$new(io___Readable__Read(self, nbytes))
  )
)

#' Create a new read/write memory mapped file of a given size
#'
#' @param path file path
#' @param size size in bytes
#' @param mode file mode (read/write/readwrite)
#'
#' @rdname mmap
#' @export
mmap_create <- `arrow::io::MemoryMappedFile`$create <- function(path, size) {
  `arrow::io::MemoryMappedFile`$new(io___MemoryMappedFile__Create(fs::path_abs(path), size))
}

#' @rdname mmap
#' @export
mmap_open <- `arrow::io::MemoryMappedFile`$open <- function(path, mode = c("read", "write", "readwrite")) {
  mode <- match(match.arg(mode), c("read", "write", "readwrite")) - 1L
  `arrow::io::MemoryMappedFile`$new(io___MemoryMappedFile__Open(fs::path_abs(path), mode))
}
