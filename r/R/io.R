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

# OutputStream ------------------------------------------------------------

`arrow::io::Writable` <- R6Class("arrow::io::Writable", inherit = `arrow::Object`,
  public = list(
    write = function(x) io___Writable__write(self, buffer(x))
  )
)

#' @title OutputStream
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#'  - `arrow::Buffer` `Read`(`int` nbytes):  Read `nbytes` bytes
#'  - `void` `close`(): close the stream
#'
#' @rdname arrow__io__OutputStream
#' @name arrow__io__OutputStream
`arrow::io::OutputStream` <- R6Class("arrow::io::OutputStream", inherit = `arrow::io::Writable`,
  public = list(
    close = function() io___OutputStream__Close(self),
    tell = function() io___OutputStream__Tell(self)
  )
)

#' @title class arrow::io::FileOutputStream
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#'  TODO
#'
#' @rdname arrow__io__FileOutputStream
#' @name arrow__io__FileOutputStream
`arrow::io::FileOutputStream` <- R6Class("arrow::io::FileOutputStream", inherit = `arrow::io::OutputStream`)

#' @title class arrow::io::MockOutputStream
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#'
#' @section Methods:
#'
#'  TODO
#'
#' @rdname arrow__io__MockOutputStream
#' @name arrow__io__MockOutputStream
`arrow::io::MockOutputStream` <- R6Class("arrow::io::MockOutputStream", inherit = `arrow::io::OutputStream`,
  public = list(
    GetExtentBytesWritten = function() io___MockOutputStream__GetExtentBytesWritten(self)
  )
)

#' @title class arrow::io::BufferOutputStream
#'
#' @usage NULL
#' @docType class
#' @section Methods:
#'
#'  TODO
#'
#' @rdname arrow__io__BufferOutputStream
#' @name arrow__io__BufferOutputStream
`arrow::io::BufferOutputStream` <- R6Class("arrow::io::BufferOutputStream", inherit = `arrow::io::OutputStream`,
  public = list(
    capacity = function() io___BufferOutputStream__capacity(self),
    getvalue = function() shared_ptr(`arrow::Buffer`, io___BufferOutputStream__Finish(self)),

    Write = function(bytes) io___BufferOutputStream__Write(self, bytes),
    Tell = function() io___BufferOutputStream__Tell(self)
  )
)

#' @title class arrow::io::FixedSizeBufferWriter
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#'
#' @section Methods:
#'
#'  TODO
#'
#' @rdname arrow__io__FixedSizeBufferWriter
#' @name arrow__io__FixedSizeBufferWriter
`arrow::io::FixedSizeBufferWriter` <- R6Class("arrow::io::FixedSizeBufferWriter", inherit = `arrow::io::OutputStream`)


# InputStream -------------------------------------------------------------

#' @title class arrow::io::Readable
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#'
#' @section Methods:
#'
#'  TODO
#'
#' @rdname arrow__io__Readable
#' @name arrow__io__Readable
`arrow::io::Readable` <- R6Class("arrow::io::Readable", inherit = `arrow::Object`,
  public = list(
    Read = function(nbytes) shared_ptr(`arrow::Buffer`, io___Readable__Read(self, nbytes))
  )
)

#' @title class arrow::io::InputStream
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#'
#' @section Methods:
#'
#'  TODO
#'
#' @rdname arrow__io__InputStream
#' @name arrow__io__InputStream
`arrow::io::InputStream` <- R6Class("arrow::io::InputStream", inherit = `arrow::io::Readable`,
  public = list(
    close = function() io___InputStream__Close(self)
  )
)

#' @title class arrow::io::RandomAccessFile
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#'
#' @section Methods:
#'
#'  TODO
#'
#' @rdname arrow__io__RandomAccessFile
#' @name arrow__io__RandomAccessFile
`arrow::io::RandomAccessFile` <- R6Class("arrow::io::RandomAccessFile", inherit = `arrow::io::InputStream`,
  public = list(
    GetSize = function() io___RandomAccessFile__GetSize(self),
    supports_zero_copy = function() io___RandomAccessFile__supports_zero_copy(self),
    Seek = function(position) io___RandomAccessFile__Seek(self, position),
    Tell = function() io___RandomAccessFile__Tell(self)
  )
)

#' @title class arrow::io::MemoryMappedFile
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#'
#' @section Methods:
#'
#'  TODO
#'
#' @seealso [mmap_open()], [mmap_create()]
#'
#'
#' @rdname arrow__io__MemoryMappedFile
#' @name arrow__io__MemoryMappedFile
`arrow::io::MemoryMappedFile` <- R6Class("arrow::io::MemoryMappedFile", inherit = `arrow::io::RandomAccessFile`,
  public = list(
    Resize = function(size) io___MemoryMappedFile__Resize(self, size)
  )
)

#' @title class arrow::io::ReadableFile
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#'
#' @section Methods:
#'
#'  TODO
#'
#' @rdname arrow__io__ReadableFile
#' @name arrow__io__ReadableFile
`arrow::io::ReadableFile` <- R6Class("arrow::io::ReadableFile", inherit = `arrow::io::RandomAccessFile`)

#' @title class arrow::io::BufferReader
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#'  TODO
#'
#' @rdname arrow__io__BufferReader
#' @name arrow__io__BufferReader
`arrow::io::BufferReader` <- R6Class("arrow::io::BufferReader", inherit = `arrow::io::RandomAccessFile`)

#' Create a new read/write memory mapped file of a given size
#'
#' @param path file path
#' @param size size in bytes
#'
#' @return a [arrow::io::MemoryMappedFile][arrow__io__MemoryMappedFile]
#'
#' @export
mmap_create <- function(path, size) {
  shared_ptr(`arrow::io::MemoryMappedFile`, io___MemoryMappedFile__Create(fs::path_abs(path), size))
}

#' Open a memory mapped file
#'
#' @param path file path
#' @param mode file mode (read/write/readwrite)
#'
#' @export
mmap_open <- function(path, mode = c("read", "write", "readwrite")) {
  mode <- match(match.arg(mode), c("read", "write", "readwrite")) - 1L
  shared_ptr(`arrow::io::MemoryMappedFile`, io___MemoryMappedFile__Open(fs::path_abs(path), mode))
}

#' open a [arrow::io::ReadableFile][arrow__io__ReadableFile]
#'
#' @param path file path
#'
#' @return a [arrow::io::ReadableFile][arrow__io__ReadableFile]
#'
#' @export
ReadableFile <- function(path) {
  shared_ptr(`arrow::io::ReadableFile`, io___ReadableFile__Open(fs::path_abs(path)))
}

#' Open a [arrow::io::FileOutputStream][arrow__io__FileOutputStream]
#'
#' @param path file path
#'
#' @return a [arrow::io::FileOutputStream][arrow__io__FileOutputStream]
#'
#' @export
FileOutputStream <- function(path) {
  shared_ptr(`arrow::io::FileOutputStream`, io___FileOutputStream__Open(path))
}

#' Open a [arrow::io::MockOutputStream][arrow__io__MockOutputStream]
#'
#' @return a [arrow::io::MockOutputStream][arrow__io__MockOutputStream]
#'
#' @export
MockOutputStream <- function() {
  shared_ptr(`arrow::io::MockOutputStream`, io___MockOutputStream__initialize())
}

#' Open a [arrow::io::BufferOutputStream][arrow__io__BufferOutputStream]
#'
#' @param initial_capacity initial capacity
#'
#' @return a [arrow::io::BufferOutputStream][arrow__io__BufferOutputStream]
#'
#' @export
BufferOutputStream <- function(initial_capacity = 0L) {
  shared_ptr(`arrow::io::BufferOutputStream`, io___BufferOutputStream__Create(initial_capacity))
}

#' Open a [arrow::io::FixedSizeBufferWriter][arrow__io__FixedSizeBufferWriter]
#'
#' @param buffer [arrow::Buffer][arrow__Buffer] or something [buffer()] can handle
#'
#' @return a [arrow::io::BufferOutputStream][arrow__io__BufferOutputStream]
#'
#' @export
FixedSizeBufferWriter <- function(buffer){
  UseMethod("FixedSizeBufferWriter")
}

#' @export
FixedSizeBufferWriter.default <- function(buffer){
  FixedSizeBufferWriter(buffer(buffer))
}

#' @export
`FixedSizeBufferWriter.arrow::Buffer` <- function(buffer){
  assert_that(buffer$is_mutable)
  shared_ptr(`arrow::io::FixedSizeBufferWriter`, io___FixedSizeBufferWriter__initialize(buffer))
}

#' Create a [arrow::io::BufferReader][arrow__io__BufferReader]
#'
#' @param x R object to treat as a buffer or a buffer created by [buffer()]
#'
#' @export
BufferReader <- function(x) {
  UseMethod("BufferReader")
}

#' @export
BufferReader.default <- function(x) {
  BufferReader(buffer(x))
}

#' @export
`BufferReader.arrow::Buffer` <- function(x) {
  shared_ptr(`arrow::io::BufferReader`, io___BufferReader__initialize(x))
}
