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

#' Read an [arrow::Table][Table] from a stream
#'
#' @param stream stream.
#'
#' - a [arrow::RecordBatchFileReader][RecordBatchFileReader]:
#'   read an [arrow::Table][Table]
#'   from all the record batches in the reader
#'
#' - a [arrow::RecordBatchStreamReader][RecordBatchStreamReader]:
#'   read an [arrow::Table][Table] from the remaining record batches
#'   in the reader
#'
#'  - a string file path: interpret the file as an arrow
#'    binary file format, and uses a [arrow::RecordBatchFileReader][RecordBatchFileReader]
#'    to process it.
#'
#'  - a raw vector: read using a [arrow::RecordBatchStreamReader][RecordBatchStreamReader]
#'
#' @return
#'
#'  - `read_table` returns an [arrow::Table][Table]
#'  - `read_arrow` returns a `data.frame`
#'
#' @details
#'
#' The methods using [arrow::RecordBatchFileReader][RecordBatchFileReader] and
#' [arrow::RecordBatchStreamReader][RecordBatchStreamReader] offer the most
#' flexibility. The other methods are for convenience.
#'
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
read_stream <- function(stream, as_data_frame = TRUE, ...) {
  if (inherits(stream, "raw")) {
    buf <- BufferReader$create(stream)
    on.exit(buf$close())
    stream <- RecordBatchStreamReader$create(buf)
  }
  assert_is(stream, "RecordBatchStreamReader")
  out <- shared_ptr(Table, Table__from_RecordBatchStreamReader(stream))
  if (as_data_frame) {
    out <- as.data.frame(out)
  }
  out
}

#' @rdname read_arrow
#' @export
read_table <- function(x, ...) {
  .Deprecated("read_arrow")
  read_arrow(x, ..., as_data_frame = FALSE)
}
