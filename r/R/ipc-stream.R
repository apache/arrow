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

#' Write Arrow IPC stream format
#'
#' Apache Arrow defines two formats for [serializing data for interprocess
#' communication
#' (IPC)](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc):
#' a "stream" format and a "file" format, known as Feather. `write_ipc_stream()`
#' and [write_feather()] write those formats, respectively.
#'
#' @inheritParams write_feather
#' @param ... extra parameters passed to `write_feather()`.
#'
#' @return `x`, invisibly.
#' @seealso [write_feather()] for writing IPC files. [write_to_raw()] to
#' serialize data to a buffer.
#' [RecordBatchWriter] for a lower-level interface.
#' @export
#' @examples
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' write_ipc_stream(mtcars, tf)
write_ipc_stream <- function(x, sink, ...) {
  x_out <- x # So we can return the data we got
  x <- as_writable_table(x)

  if (!inherits(sink, "OutputStream")) {
    sink <- make_output_stream(sink)
    on.exit(sink$close())
  }

  writer <- RecordBatchStreamWriter$create(sink, x$schema)
  writer$write(x)
  writer$close()

  invisible(x_out)
}

#' Write Arrow data to a raw vector
#'
#' [write_ipc_stream()] and [write_feather()] write data to a sink and return
#' the data (`data.frame`, `RecordBatch`, or `Table`) they were given.
#' This function wraps those so that you can serialize data to a buffer and
#' access that buffer as a `raw` vector in R.
#' @inheritParams write_feather
#' @param format one of `c("stream", "file")`, indicating the IPC format to use
#' @return A `raw` vector containing the bytes of the IPC serialized data.
#' @examples
#' # The default format is "stream"
#' mtcars_raw <- write_to_raw(mtcars)
#' @export
write_to_raw <- function(x, format = c("stream", "file")) {
  sink <- BufferOutputStream$create()
  if (match.arg(format) == "stream") {
    write_ipc_stream(x, sink)
  } else {
    write_feather(x, sink)
  }
  as.raw(buffer(sink))
}

#' Read Arrow IPC stream format
#'
#' Apache Arrow defines two formats for [serializing data for interprocess
#' communication
#' (IPC)](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc):
#' a "stream" format and a "file" format, known as Feather. `read_ipc_stream()`
#' and [read_feather()] read those formats, respectively.
#'
#' @param file A character file name or URI, `raw` vector, an Arrow input stream,
#' or a `FileSystem` with path (`SubTreeFileSystem`).
#' If a file name or URI, an Arrow [InputStream] will be opened and
#' closed when finished. If an input stream is provided, it will be left
#' open.
#' @param as_data_frame Should the function return a `data.frame` (default) or
#' an Arrow [Table]?
#' @param ... extra parameters passed to `read_feather()`.
#'
#' @return A `data.frame` if `as_data_frame` is `TRUE` (the default), or an
#' Arrow [Table] otherwise
#' @seealso [write_feather()] for writing IPC files. [RecordBatchReader] for a
#' lower-level interface.
#' @export
read_ipc_stream <- function(file, as_data_frame = TRUE, ...) {
  if (!inherits(file, "InputStream")) {
    file <- make_readable_file(file, random_access = FALSE)
    on.exit(file$close())
  }

  # TODO: this could take col_select, like the other readers
  # https://issues.apache.org/jira/browse/ARROW-6830
  out <- RecordBatchStreamReader$create(file)$read_table()
  if (as_data_frame) {
    out <- collect.ArrowTabular(out)
  }
  out
}
