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

`arrow::io::Readable` <- R6Class("arrow::io::Readable", inherit = `arrow::Object`,
  public = list(
    Read = function(nbytes) `arrow::Buffer`$new(io___Readable__Read(self, nbytes))
  )
)

`arrow::io::InputStream` <- R6Class("arrow::io::InputStream", inherit = `arrow::io::Readable`,
  public = list(
    Close = function() io___InputStream__Close(self)
  )
)

`arrow::io::Writable` <- R6Class("arrow::io::Writable", inherit = `arrow::Object`)

`arrow::io::OutputStream` <- R6Class("arrow::io::OutputStream", inherit = `arrow::io::Writable`,
  public = list(
    Close = function() io___OutputStream__Close(self)
  )
)

`arrow::io::FileOutputStream` <- R6Class("arrow::io::FileOutputStream", inherit = `arrow::io::OutputStream`)

`arrow::io::MockOutputStream` <- R6Class("arrow::io::MockOutputStream", inherit = `arrow::io::OutputStream`,
  public = list(
    GetExtentBytesWritten = function() io___MockOutputStream__GetExtentBytesWritten(self)
  )
)

`arrow::io::BufferOutputStream` <- R6Class("arrow::io::BufferOutputStream", inherit = `arrow::io::OutputStream`,
  public = list(
    capacity = function() io___BufferOutputStream__capacity(self),
    Finish = function() `arrow::Buffer`$new(io___BufferOutputStream__Finish(self)),
    Write = function(bytes) io___BufferOutputStream__Write(self, bytes),
    Tell = function() io___BufferOutputStream__Tell(self)
  )
)

`arrow::io::FixedSizeBufferWriter` <- R6Class("arrow::io::FixedSizeBufferWriter", inherit = `arrow::io::OutputStream`)

`arrow::io::RandomAccessFile` <- R6Class("arrow::io::RandomAccessFile", inherit = `arrow::io::InputStream`,
  public = list(
    GetSize = function() io___RandomAccessFile__GetSize(self),
    supports_zero_copy = function() io___RandomAccessFile__supports_zero_copy(self),
    Seek = function(position) io___RandomAccessFile__Seek(self, position),
    Tell = function() io___RandomAccessFile__Tell(self)
  )
)

`arrow::io::MemoryMappedFile` <- R6Class("arrow::io::MemoryMappedFile", inherit = `arrow::io::RandomAccessFile`,
  public = list(
    Resize = function(size) io___MemoryMappedFile__Resize(self, size)
  )
)

`arrow::io::ReadableFile` <- R6Class("arrow::io::ReadableFile", inherit = `arrow::io::RandomAccessFile`)
`arrow::io::BufferReader` <- R6Class("arrow::io::BufferReader", inherit = `arrow::io::RandomAccessFile`)


#' Create a new read/write memory mapped file of a given size
#'
#' @param path file path
#' @param size size in bytes
#' @param mode file mode (read/write/readwrite)
#' @param buffer an `arrow::Buffer`, typically created by [buffer()]
#' @param initial_capacity initial capacity for the buffer output stream
#'
#' @rdname io
#' @export
mmap_create <- `arrow::io::MemoryMappedFile`$create <- function(path, size) {
  `arrow::io::MemoryMappedFile`$new(io___MemoryMappedFile__Create(fs::path_abs(path), size))
}

#' @rdname io
#' @export
mmap_open <- `arrow::io::MemoryMappedFile`$open <- function(path, mode = c("read", "write", "readwrite")) {
  mode <- match(match.arg(mode), c("read", "write", "readwrite")) - 1L
  `arrow::io::MemoryMappedFile`$new(io___MemoryMappedFile__Open(fs::path_abs(path), mode))
}

#' @rdname io
#' @export
file_open <- `arrow::io::ReadableFile`$open <- function(path) {
  `arrow::io::ReadableFile`$new(io___ReadableFile__Open(fs::path_abs(path)))
}

#' @rdname io
#' @export
file_output_stream <- function(path) {
  `arrow::io::FileOutputStream`$new(io___FileOutputStream__Open(path))
}

#' @rdname io
#' @export
mock_output_stream <- function() {
  `arrow::io::MockOutputStream`$new(io___MockOutputStream__initialize())
}

#' @rdname io
#' @export
buffer_output_stream <- function(initial_capacity = 0L) {
  `arrow::io::BufferOutputStream`$new(io___BufferOutputStream__Create(initial_capacity))
}

#' @rdname io
#' @export
fixed_size_buffer_writer <- function(buffer){
  UseMethod("fixed_size_buffer_writer")
}

#' @export
fixed_size_buffer_writer.default <- function(buffer){
  fixed_size_buffer_writer(buffer(buffer))
}

#' @export
`fixed_size_buffer_writer.arrow::Buffer` <- function(buffer){
  assert_that(buffer$is_mutable())
  `arrow::io::FixedSizeBufferWriter`$new(io___FixedSizeBufferWriter__initialize(buffer))
}

#' Create a `arrow::BufferReader`
#'
#' @param x R object to treat as a buffer or a buffer created by [buffer()]
#'
#' @export
buffer_reader <- function(x) {
  UseMethod("buffer_reader")
}

#' @export
`buffer_reader.arrow::Buffer` <- function(x) {
  `arrow::io::BufferReader`$new(io___BufferReader__initialize(x))
}

#' @export
buffer_reader.default <- function(x) {
  buffer_reader(buffer(x))
}

