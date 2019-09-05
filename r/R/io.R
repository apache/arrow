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
#' @include enums.R
#' @include buffer.R

# OutputStream ------------------------------------------------------------

Writable <- R6Class("Writable", inherit = Object,
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
#'  - Buffer `Read`(`int` nbytes):  Read `nbytes` bytes
#'  - `void` `close`(): close the stream
#'
#' @rdname arrow__io__OutputStream
#' @name arrow__io__OutputStream
OutputStream <- R6Class("OutputStream", inherit = Writable,
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
FileOutputStream <- R6Class("FileOutputStream", inherit = OutputStream)

FileOutputStream$create <- function(path) {
  path <- normalizePath(path, mustWork = FALSE)
  shared_ptr(FileOutputStream, io___FileOutputStream__Open(path))
}

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
MockOutputStream <- R6Class("MockOutputStream", inherit = OutputStream,
  public = list(
    GetExtentBytesWritten = function() io___MockOutputStream__GetExtentBytesWritten(self)
  )
)

MockOutputStream$create <- function() {
  shared_ptr(MockOutputStream, io___MockOutputStream__initialize())
}

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
BufferOutputStream <- R6Class("BufferOutputStream", inherit = OutputStream,
  public = list(
    capacity = function() io___BufferOutputStream__capacity(self),
    getvalue = function() shared_ptr(Buffer, io___BufferOutputStream__Finish(self)),

    Write = function(bytes) io___BufferOutputStream__Write(self, bytes),
    Tell = function() io___BufferOutputStream__Tell(self)
  )
)

BufferOutputStream$create <- function(initial_capacity = 0L) {
  shared_ptr(BufferOutputStream, io___BufferOutputStream__Create(initial_capacity))
}

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
FixedSizeBufferWriter <- R6Class("FixedSizeBufferWriter", inherit = OutputStream)

FixedSizeBufferWriter$create <- function(x) {
  x <- buffer(x)
  assert_that(x$is_mutable)
  shared_ptr(FixedSizeBufferWriter, io___FixedSizeBufferWriter__initialize(x))
}

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
Readable <- R6Class("Readable", inherit = Object,
  public = list(
    Read = function(nbytes) shared_ptr(Buffer, io___Readable__Read(self, nbytes))
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
InputStream <- R6Class("InputStream", inherit = Readable,
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
RandomAccessFile <- R6Class("RandomAccessFile", inherit = InputStream,
  public = list(
    GetSize = function() io___RandomAccessFile__GetSize(self),
    supports_zero_copy = function() io___RandomAccessFile__supports_zero_copy(self),
    Seek = function(position) io___RandomAccessFile__Seek(self, position),
    Tell = function() io___RandomAccessFile__Tell(self),

    Read = function(nbytes = NULL) {
      if (is.null(nbytes)) {
        shared_ptr(Buffer, io___RandomAccessFile__Read0(self))
      } else {
        shared_ptr(Buffer, io___Readable__Read(self, nbytes))
      }
    },

    ReadAt = function(position, nbytes = NULL) {
      if (is.null(nbytes)) {
        nbytes <- self$GetSize() - position
      }
      shared_ptr(Buffer, io___RandomAccessFile__ReadAt(self, position, nbytes))
    }
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
MemoryMappedFile <- R6Class("MemoryMappedFile", inherit = RandomAccessFile,
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
ReadableFile <- R6Class("ReadableFile", inherit = RandomAccessFile)

ReadableFile$create <- function(path) {
  shared_ptr(ReadableFile, io___ReadableFile__Open(normalizePath(path)))
}

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
BufferReader <- R6Class("BufferReader", inherit = RandomAccessFile)

BufferReader$create <- function(x) {
  x <- buffer(x)
  shared_ptr(BufferReader, io___BufferReader__initialize(x))
}

#' Create a new read/write memory mapped file of a given size
#'
#' @param path file path
#' @param size size in bytes
#'
#' @return a [arrow::io::MemoryMappedFile][arrow__io__MemoryMappedFile]
#'
#' @export
mmap_create <- function(path, size) {
  path <- normalizePath(path, mustWork = FALSE)
  shared_ptr(MemoryMappedFile, io___MemoryMappedFile__Create(path, size))
}

#' Open a memory mapped file
#'
#' @param path file path
#' @param mode file mode (read/write/readwrite)
#'
#' @export
mmap_open <- function(path, mode = c("read", "write", "readwrite")) {
  mode <- match(match.arg(mode), c("read", "write", "readwrite")) - 1L
  path <- normalizePath(path)
  shared_ptr(MemoryMappedFile, io___MemoryMappedFile__Open(path, mode))
}
