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

#' @title OutputStream classes
#' @description `FileOutputStream` is for writing to a file;
#' `BufferOutputStream` and `FixedSizeBufferWriter` write to buffers;
#' `MockOutputStream` just reports back how many bytes it received, for testing
#' purposes. You can create one and pass it to any of the table writers, for
#' example.
#' @usage NULL
#' @format NULL
#' @docType class
#' @section Factory:
#'
#' The `$create()` factory methods instantiate the `OutputStream` object and
#' take the following arguments, depending on the subclass:
#'
#' - `path` For `FileOutputStream`, a character file name
#' - `initial_capacity` For `BufferOutputStream`, the size in bytes of the
#'    buffer.
#' - `x` For `FixedSizeBufferWriter`, a [Buffer] or an object that can be
#'    made into a buffer via `buffer()`.
#'
#' `MockOutputStream$create()` does not take any arguments.
#'
#' @section Methods:
#'
#'  - `$tell()`: return the position in the stream
#'  - `$close()`: close the stream
#'  - `$write(x)`: send `x` to the stream
#'  - `$capacity()`: for `BufferOutputStream`
#'  - `$getvalue()`: for `BufferOutputStream`
#'  - `$GetExtentBytesWritten()`: for `MockOutputStream`, report how many bytes
#'    were sent.
#'
#' @rdname OutputStream
#' @name OutputStream
OutputStream <- R6Class("OutputStream", inherit = Writable,
  public = list(
    close = function() io___OutputStream__Close(self),
    tell = function() io___OutputStream__Tell(self)
  )
)

#' @usage NULL
#' @format NULL
#' @rdname OutputStream
#' @export
FileOutputStream <- R6Class("FileOutputStream", inherit = OutputStream)
FileOutputStream$create <- function(path) {
  path <- normalizePath(path, mustWork = FALSE)
  shared_ptr(FileOutputStream, io___FileOutputStream__Open(path))
}

#' @usage NULL
#' @format NULL
#' @rdname OutputStream
#' @export
MockOutputStream <- R6Class("MockOutputStream", inherit = OutputStream,
  public = list(
    GetExtentBytesWritten = function() io___MockOutputStream__GetExtentBytesWritten(self)
  )
)
MockOutputStream$create <- function() {
  shared_ptr(MockOutputStream, io___MockOutputStream__initialize())
}

#' @usage NULL
#' @format NULL
#' @rdname OutputStream
#' @export
BufferOutputStream <- R6Class("BufferOutputStream", inherit = OutputStream,
  public = list(
    capacity = function() io___BufferOutputStream__capacity(self),
    getvalue = function() shared_ptr(Buffer, io___BufferOutputStream__Finish(self)),
    write = function(bytes) io___BufferOutputStream__Write(self, bytes),
    tell = function() io___BufferOutputStream__Tell(self)
  )
)
BufferOutputStream$create <- function(initial_capacity = 0L) {
  shared_ptr(BufferOutputStream, io___BufferOutputStream__Create(initial_capacity))
}

#' @usage NULL
#' @format NULL
#' @rdname OutputStream
#' @export
FixedSizeBufferWriter <- R6Class("FixedSizeBufferWriter", inherit = OutputStream)
FixedSizeBufferWriter$create <- function(x) {
  x <- buffer(x)
  assert_that(x$is_mutable)
  shared_ptr(FixedSizeBufferWriter, io___FixedSizeBufferWriter__initialize(x))
}

# InputStream -------------------------------------------------------------


Readable <- R6Class("Readable", inherit = Object,
  public = list(
    Read = function(nbytes) shared_ptr(Buffer, io___Readable__Read(self, nbytes))
  )
)

#' @title InputStream classes
#' @description `RandomAccessFile` inherits from `InputStream` and is a base
#' class for: `ReadableFile` for reading from a file; `MemoryMappedFile` for
#' the same but with memory mapping; and `BufferReader` for reading from a
#' buffer. Use these with the various table readers.
#' @usage NULL
#' @format NULL
#' @docType class
#' @section Factory:
#'
#' The `$create()` factory methods instantiate the `InputStream` object and
#' take the following arguments, depending on the subclass:
#'
#' - `path` For `ReadableFile`, a character file name
#' - `x` For `BufferReader`, a [Buffer] or an object that can be
#'    made into a buffer via `buffer()`.
#'
#' To instantiate a `MemoryMappedFile`, call [mmap_open()].
#'
#' @section Methods:
#'
#'  - `$GetSize()`: 
#'  - `$supports_zero_copy()`: Logical
#'  - `$seek(position)`: go to that position in the stream
#'  - `$tell()`: return the position in the stream
#'  - `$close()`: close the stream
#'  - `$Read(nbytes)`: read data from the stream, either a specified `nbytes` or
#'    all, if `nbytes` is not provided
#'  - `$ReadAt(position, nbytes)`: similar to `$seek(position)$Read(nbytes)`
#'  - `$Resize(size)`: for a `MemoryMappedFile` that is writeable
#'
#' @rdname InputStream
#' @name InputStream
InputStream <- R6Class("InputStream", inherit = Readable,
  public = list(
    close = function() io___InputStream__Close(self)
  )
)

#' @usage NULL
#' @format NULL
#' @rdname InputStream
#' @export
RandomAccessFile <- R6Class("RandomAccessFile", inherit = InputStream,
  public = list(
    GetSize = function() io___RandomAccessFile__GetSize(self),
    supports_zero_copy = function() io___RandomAccessFile__supports_zero_copy(self),
    seek = function(position) io___RandomAccessFile__Seek(self, position),
    tell = function() io___RandomAccessFile__Tell(self),

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

#' @usage NULL
#' @format NULL
#' @rdname InputStream
#' @export
MemoryMappedFile <- R6Class("MemoryMappedFile", inherit = RandomAccessFile,
  public = list(
    Resize = function(size) io___MemoryMappedFile__Resize(self, size)
  )
)

#' @usage NULL
#' @format NULL
#' @rdname InputStream
#' @export
ReadableFile <- R6Class("ReadableFile", inherit = RandomAccessFile)
ReadableFile$create <- function(path) {
  shared_ptr(ReadableFile, io___ReadableFile__Open(normalizePath(path)))
}

#' @usage NULL
#' @format NULL
#' @rdname InputStream
#' @export
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
#' @return a [arrow::io::MemoryMappedFile][MemoryMappedFile]
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

#' Handle a range of possible input sources
#' @param file A character file name, raw vector, or an Arrow input stream
#' @param mmap Logical: whether to memory-map the file (default `TRUE`)
#' @return An `InputStream` or a subclass of one.
#' @keywords internal
make_readable_file <- function(file, mmap = TRUE) {
  if (is.character(file)) {
    assert_that(length(file) == 1L)
    if (isTRUE(mmap)) {
      file <- mmap_open(file)
    } else {
      file <- ReadableFile$create(file)
    }
  } else if (inherits(file, c("raw", "Buffer"))) {
    file <- BufferReader$create(file)
  }
  assert_is(file, "InputStream")
  file
}
