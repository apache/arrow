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


#' @title RecordBatchWriter classes
#' @description Apache Arrow defines two formats for [serializing data for interprocess
#' communication
#' (IPC)](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc):
#' a "stream" format and a "file" format, known as Feather.
#' `RecordBatchStreamWriter` and `RecordBatchFileWriter` are
#' interfaces for writing record batches to those formats, respectively.
#'
#' For guidance on how to use these classes, see the examples section.
#'
#' @seealso [write_ipc_stream()] and [write_feather()] provide a much simpler
#' interface for writing data to these formats and are sufficient for many use
#' cases. [write_to_raw()] is a version that serializes data to a buffer.
#' @usage NULL
#' @format NULL
#' @docType class
#' @section Factory:
#'
#' The `RecordBatchFileWriter$create()` and `RecordBatchStreamWriter$create()`
#' factory methods instantiate the object and take the following arguments:
#'
#' - `sink` An `OutputStream`
#' - `schema` A [Schema] for the data to be written
#' - `use_legacy_format` logical: write data formatted so that Arrow libraries
#'   versions 0.14 and lower can read it. Default is `FALSE`. You can also
#'   enable this by setting the environment variable `ARROW_PRE_0_15_IPC_FORMAT=1`.
#' - `metadata_version`: A string like "V5" or the equivalent integer indicating
#'   the Arrow IPC MetadataVersion. Default (NULL) will use the latest version,
#'   unless the environment variable `ARROW_PRE_1_0_METADATA_VERSION=1`, in
#'   which case it will be V4.
#'
#' @section Methods:
#'
#' - `$write(x)`: Write a [RecordBatch], [Table], or `data.frame`, dispatching
#'    to the methods below appropriately
#' - `$write_batch(batch)`: Write a `RecordBatch` to stream
#' - `$write_table(table)`: Write a `Table` to stream
#' - `$close()`: close stream. Note that this indicates end-of-file or
#'   end-of-stream--it does not close the connection to the `sink`. That needs
#'   to be closed separately.
#'
#' @rdname RecordBatchWriter
#' @name RecordBatchWriter
#' @include arrow-package.R
#' @examplesIf arrow_available()
#' tf <- tempfile()
#' on.exit(unlink(tf))
#'
#' batch <- record_batch(chickwts)
#'
#' # This opens a connection to the file in Arrow
#' file_obj <- FileOutputStream$create(tf)
#' # Pass that to a RecordBatchWriter to write data conforming to a schema
#' writer <- RecordBatchFileWriter$create(file_obj, batch$schema)
#' writer$write(batch)
#' # You may write additional batches to the stream, provided that they have
#' # the same schema.
#' # Call "close" on the writer to indicate end-of-file/stream
#' writer$close()
#' # Then, close the connection--closing the IPC message does not close the file
#' file_obj$close()
#'
#' # Now, we have a file we can read from. Same pattern: open file connection,
#' # then pass it to a RecordBatchReader
#' read_file_obj <- ReadableFile$create(tf)
#' reader <- RecordBatchFileReader$create(read_file_obj)
#' # RecordBatchFileReader knows how many batches it has (StreamReader does not)
#' reader$num_record_batches
#' # We could consume the Reader by calling $read_next_batch() until all are,
#' # consumed, or we can call $read_table() to pull them all into a Table
#' tab <- reader$read_table()
#' # Call as.data.frame to turn that Table into an R data.frame
#' df <- as.data.frame(tab)
#' # This should be the same data we sent
#' all.equal(df, chickwts, check.attributes = FALSE)
#' # Unlike the Writers, we don't have to close RecordBatchReaders,
#' # but we do still need to close the file connection
#' read_file_obj$close()
RecordBatchWriter <- R6Class("RecordBatchWriter",
  inherit = ArrowObject,
  public = list(
    write_batch = function(batch) ipc___RecordBatchWriter__WriteRecordBatch(self, batch),
    write_table = function(table) ipc___RecordBatchWriter__WriteTable(self, table),
    write = function(x) {
      if (inherits(x, "RecordBatch")) {
        self$write_batch(x)
      } else if (inherits(x, "Table")) {
        self$write_table(x)
      } else {
        self$write_table(as_arrow_table(x))
      }
    },
    close = function() ipc___RecordBatchWriter__Close(self)
  )
)

#' @usage NULL
#' @format NULL
#' @rdname RecordBatchWriter
#' @export
RecordBatchStreamWriter <- R6Class("RecordBatchStreamWriter", inherit = RecordBatchWriter)
RecordBatchStreamWriter$create <- function(sink,
                                           schema,
                                           use_legacy_format = NULL,
                                           metadata_version = NULL) {
  if (is.string(sink)) {
    stop(
      "RecordBatchStreamWriter$create() requires an Arrow InputStream. ",
      "Try providing FileOutputStream$create(", substitute(sink), ")",
      call. = FALSE
    )
  }
  assert_is(sink, "OutputStream")
  assert_is(schema, "Schema")

  ipc___RecordBatchStreamWriter__Open(
    sink,
    schema,
    get_ipc_use_legacy_format(use_legacy_format),
    get_ipc_metadata_version(metadata_version)
  )
}

#' @usage NULL
#' @format NULL
#' @rdname RecordBatchWriter
#' @export
RecordBatchFileWriter <- R6Class("RecordBatchFileWriter", inherit = RecordBatchStreamWriter)
RecordBatchFileWriter$create <- function(sink,
                                         schema,
                                         use_legacy_format = NULL,
                                         metadata_version = NULL) {
  if (is.string(sink)) {
    stop(
      "RecordBatchFileWriter$create() requires an Arrow InputStream. ",
      "Try providing FileOutputStream$create(", substitute(sink), ")",
      call. = FALSE
    )
  }
  assert_is(sink, "OutputStream")
  assert_is(schema, "Schema")

  ipc___RecordBatchFileWriter__Open(
    sink,
    schema,
    get_ipc_use_legacy_format(use_legacy_format),
    get_ipc_metadata_version(metadata_version)
  )
}

get_ipc_metadata_version <- function(x) {
  input <- x
  if (is_integerish(x)) {
    # 4 means "V4", which actually happens to be 3L
    x <- paste0("V", x)
  } else if (is.null(x)) {
    if (identical(Sys.getenv("ARROW_PRE_1_0_METADATA_VERSION"), "1") ||
      identical(Sys.getenv("ARROW_PRE_0_15_IPC_FORMAT"), "1")) {
      # PRE_1_0 is specific for this;
      # if you already set PRE_0_15, PRE_1_0 should be implied
      x <- "V4"
    } else {
      # Take the latest
      x <- length(MetadataVersion)
    }
  }
  out <- MetadataVersion[[x]]
  if (is.null(out)) {
    stop(deparse(input), " is not a valid IPC MetadataVersion", call. = FALSE)
  }
  out
}

get_ipc_use_legacy_format <- function(x) {
  isTRUE(x %||% identical(Sys.getenv("ARROW_PRE_0_15_IPC_FORMAT"), "1"))
}
