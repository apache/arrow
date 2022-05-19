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
#' @include enums.R
#' @include buffer.R

# OutputStream ------------------------------------------------------------

Writable <- R6Class("Writable",
  inherit = ArrowObject,
  public = list(
    write = function(x) io___Writable__write(self, buffer(x))
  )
)

#' @title OutputStream classes
#' @description `FileOutputStream` is for writing to a file;
#' `BufferOutputStream` writes to a buffer;
#' You can create one and pass it to any of the table writers, for example.
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
#'
#' @section Methods:
#'
#'  - `$tell()`: return the position in the stream
#'  - `$close()`: close the stream
#'  - `$write(x)`: send `x` to the stream
#'  - `$capacity()`: for `BufferOutputStream`
#'  - `$finish()`: for `BufferOutputStream`
#'  - `$GetExtentBytesWritten()`: for `MockOutputStream`, report how many bytes
#'    were sent.
#'
#' @rdname OutputStream
#' @name OutputStream
OutputStream <- R6Class("OutputStream",
  inherit = Writable,
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
  io___FileOutputStream__Open(clean_path_abs(path))
}

#' @usage NULL
#' @format NULL
#' @rdname OutputStream
#' @export
BufferOutputStream <- R6Class("BufferOutputStream",
  inherit = OutputStream,
  public = list(
    capacity = function() io___BufferOutputStream__capacity(self),
    finish = function() io___BufferOutputStream__Finish(self),
    write = function(bytes) io___BufferOutputStream__Write(self, bytes),
    tell = function() io___BufferOutputStream__Tell(self)
  )
)
BufferOutputStream$create <- function(initial_capacity = 0L) {
  io___BufferOutputStream__Create(initial_capacity)
}

# InputStream -------------------------------------------------------------


Readable <- R6Class("Readable",
  inherit = ArrowObject,
  public = list(
    Read = function(nbytes) io___Readable__Read(self, nbytes)
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
InputStream <- R6Class("InputStream",
  inherit = Readable,
  public = list(
    close = function() io___InputStream__Close(self)
  )
)

#' @usage NULL
#' @format NULL
#' @rdname InputStream
#' @export
RandomAccessFile <- R6Class("RandomAccessFile",
  inherit = InputStream,
  public = list(
    GetSize = function() io___RandomAccessFile__GetSize(self),
    supports_zero_copy = function() io___RandomAccessFile__supports_zero_copy(self),
    seek = function(position) io___RandomAccessFile__Seek(self, position),
    tell = function() io___RandomAccessFile__Tell(self),
    Read = function(nbytes = NULL) {
      if (is.null(nbytes)) {
        io___RandomAccessFile__Read0(self)
      } else {
        io___Readable__Read(self, nbytes)
      }
    },
    ReadAt = function(position, nbytes = NULL) {
      if (is.null(nbytes)) {
        nbytes <- self$GetSize() - position
      }
      io___RandomAccessFile__ReadAt(self, position, nbytes)
    },
    ReadMetadata = function() {
      as.list(io___RandomAccessFile__ReadMetadata(self))
    }
  )
)

#' @usage NULL
#' @format NULL
#' @rdname InputStream
#' @export
MemoryMappedFile <- R6Class("MemoryMappedFile",
  inherit = RandomAccessFile,
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
  io___ReadableFile__Open(clean_path_abs(path))
}

#' @usage NULL
#' @format NULL
#' @rdname InputStream
#' @export
BufferReader <- R6Class("BufferReader", inherit = RandomAccessFile)
BufferReader$create <- function(x) {
  x <- buffer(x)
  io___BufferReader__initialize(x)
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
  path <- clean_path_abs(path)
  io___MemoryMappedFile__Create(path, size)
}

#' Open a memory mapped file
#'
#' @param path file path
#' @param mode file mode (read/write/readwrite)
#'
#' @export
mmap_open <- function(path, mode = c("read", "write", "readwrite")) {
  mode <- match(match.arg(mode), c("read", "write", "readwrite")) - 1L
  path <- clean_path_abs(path)
  io___MemoryMappedFile__Open(path, mode)
}

#' Handle a range of possible input sources
#' @param file A character file name, `raw` vector, or an Arrow input stream
#' @param mmap Logical: whether to memory-map the file (default `TRUE`)
#' @param compression If the file is compressed, created a [CompressedInputStream]
#' with this compression codec, either a [Codec] or the string name of one.
#' If `NULL` (default) and `file` is a string file name, the function will try
#' to infer compression from the file extension.
#' @param filesystem If not `NULL`, `file` will be opened via the
#' `filesystem$OpenInputFile()` filesystem method, rather than the `io` module's
#' `MemoryMappedFile` or `ReadableFile` constructors.
#' @return An `InputStream` or a subclass of one.
#' @keywords internal
make_readable_file <- function(file, mmap = TRUE, compression = NULL, filesystem = NULL) {
  if (inherits(file, "SubTreeFileSystem")) {
    filesystem <- file$base_fs
    file <- file$base_path
  }
  if (is.string(file)) {
    if (is_url(file)) {
      file <- tryCatch(
        {
          fs_and_path <- FileSystem$from_uri(file)
          filesystem <- fs_and_path$fs
          fs_and_path$path
        },
        error = function(e) {
          MakeRConnectionInputStream(url(file, open = "rb"))
        }
      )
    }

    if (is.null(compression)) {
      # Infer compression from the file path
      compression <- detect_compression(file)
    }

    if (!is.null(filesystem)) {
      file <- filesystem$OpenInputFile(file)
    } else if (is.string(file) && isTRUE(mmap)) {
      file <- mmap_open(file)
    } else if (is.string(file)) {
      file <- ReadableFile$create(file)
    }

    if (is_compressed(compression)) {
      file <- CompressedInputStream$create(file, compression)
    }
  } else if (inherits(file, c("raw", "Buffer"))) {
    file <- BufferReader$create(file)
  } else if (inherits(file, "connection")) {
    if (!isOpen(file)) {
      open(file, "rb")
    }

    # Try to create a RandomAccessFile first because some readers need this
    # (e.g., feather, parquet) but fall back on an InputStream for the readers
    # that don't (e.g., IPC, CSV)
    file <- tryCatch(
      MakeRConnectionRandomAccessFile(file),
      error = function(e) MakeRConnectionInputStream(file)
    )
  }
  assert_is(file, "InputStream")
  file
}

make_output_stream <- function(x, filesystem = NULL, compression = NULL) {
  if (inherits(x, "connection")) {
    if (!isOpen(x)) {
      open(x, "wb")
    }

    return(MakeRConnectionOutputStream(x))
  }

  if (inherits(x, "SubTreeFileSystem")) {
    filesystem <- x$base_fs
    x <- x$base_path
  } else if (is_url(x)) {
    fs_and_path <- FileSystem$from_uri(x)
    filesystem <- fs_and_path$fs
    x <- fs_and_path$path
  }

  if (is.null(compression)) {
    # Infer compression from sink
    compression <- detect_compression(x)
  }

  assert_that(is.string(x))
  if (is.null(filesystem) && is_compressed(compression)) {
    CompressedOutputStream$create(x) ##compressed local
  } else if (is.null(filesystem) && !is_compressed(compression)) {
    FileOutputStream$create(x) ## uncompressed local
  } else if (!is.null(filesystem) && is_compressed(compression)) {
    CompressedOutputStream$create(filesystem$OpenOutputStream(x)) ## compressed remote
  } else {
    filesystem$OpenOutputStream(x) ## uncompressed remote
  }
}

detect_compression <- function(path) {
  if (!is.string(path)) {
    return("uncompressed")
  }

  # Remove any trailing slashes, which FileSystem$from_uri may add
  path <- gsub("/$", "", path)

  switch(tools::file_ext(path),
    bz2 = "bz2",
    gz = "gzip",
    lz4 = "lz4",
    zst = "zstd",
    "uncompressed"
  )
}
