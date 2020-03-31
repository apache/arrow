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

#' Write Arrow formatted data
#'
#' Apache Arrow defines two formats for [serializing data for interprocess
#' communication (IPC)](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc):
#' a "stream" format and a "file" format, known as Feather. `write_arrow()`
#' is a convenience wrapper around `write_stream()` and [write_feather()], which
#' write those formats, respectively.
#'
#' @param x an Arrow [Table] or [RecordBatch], or a `data.frame`
#' @param sink string file path, buffer, or Arrow C++ class to write to. If
#' `write_arrow()` receives a `RecordBatchStreamWriter` or an empty R `raw` vector,
#' it will dispatch to `write_stream()`; otherwise, it calls `write_feather()`
#' to write a file.
#' @param ... extra parameters passed to `write_feather()`.
#'
#' @return `write_stream()` returns the stream: either the
#' `RecordBatchStreamWriter` passed to `sink`, connection still open, or if
#' `sink` is a `raw` vector, a new `raw` vector containing the bytes that were
#' written using a `RecordBatchStreamWriter`. `write_feather()` returns `x`,
#' invisibly.
#' @seealso [RecordBatchWriter]
#' @export
write_arrow <- function(x, sink, ...) {
  if (inherits(sink, c("RecordBatchStreamWriter", "raw"))) {
    write_stream(x, sink, ...)
  } else {
    write_feather(x, sink, ...)
  }
}

#' @rdname write_arrow
#' @export
write_stream <- function(x, sink, ...) {
  if (inherits(sink, "raw")) {
    if (is.data.frame(x)) {
      x <- Table$create(x)
    }

    n <- count_bytes_to_serialize(x)
    # now that we know the size, stream in a buffer backed by an R raw vector
    bytes <- raw(n)
    buffer_writer <- FixedSizeBufferWriter$create(buffer(bytes))
    sink <- RecordBatchStreamWriter$create(buffer_writer, x$schema)
    on.exit(sink$close())
    sink$write(x)
    # Note that this returns a new R raw vector, not the one passed as `sink`
    # Nor is it returning an Arrow C++ object
    bytes
  } else {
    assert_is(sink, "RecordBatchStreamWriter")
    sink$write(x)
  }
}

count_bytes_to_serialize <- function(x) {
  mock_stream <- MockOutputStream$create()
  writer <- RecordBatchStreamWriter$create(mock_stream, x$schema)
  writer$write(x)
  writer$close()
  mock_stream$GetExtentBytesWritten()
}

#' Read Arrow formatted data
#'
#' Apache Arrow defines two formats for [serializing data for interprocess
#' communication (IPC)](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc):
#' a "stream" format and a "file" format, known as Feather. `read_arrow()`
#' is a convenience wrapper around `read_stream()` and [read_feather()], which
#' write those formats, respectively.
#'
#' @param x string file path, buffer, or Arrow C++ class to read from. If
#' `read_arrow()` receives a `RecordBatchStreamReader` or a R `raw` vector,
#' it will dispatch to `read_stream()`; otherwise, it calls `read_feather()`
#' to write a file.
#' @param as_data_frame Should the function return a `data.frame` (default) or
#' an Arrow [Table]?
#' @param ... extra parameters passed to `read_feather()`.
#'
#' @return A `data.frame` if `as_data_frame` is `TRUE` (the default), or an
#' Arrow [Table] otherwise
#' @seealso [RecordBatchReader]
#' @export
read_arrow <- function(x, ...) {
  if (inherits(x, c("RecordBatchStreamReader", "raw"))) {
    read_stream(x, ...)
  } else {
    read_feather(x, ...)
  }
}

#' @rdname read_arrow
#' @export
read_stream <- function(x, as_data_frame = TRUE, ...) {
  if (inherits(x, "raw")) {
    buf <- BufferReader$create(x)
    on.exit(buf$close())
    x <- RecordBatchStreamReader$create(buf)
  }
  assert_is(x, "RecordBatchStreamReader")
  out <- shared_ptr(Table, Table__from_RecordBatchStreamReader(x))
  if (as_data_frame) {
    out <- as.data.frame(out)
  }
  out
}
