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

#' @title class arrow::RecordBatchReader
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname arrow__RecordBatchReader
#' @name arrow__RecordBatchReader
`arrow::RecordBatchReader` <- R6Class("arrow::RecordBatchReader", inherit = `arrow::Object`,
  public = list(
    read_next_batch = function() {
      shared_ptr(`arrow::RecordBatch`, RecordBatchReader__ReadNext(self))
    }
  ),
  active = list(
    schema = function() shared_ptr(`arrow::Schema`, RecordBatchReader__schema(self))
  )
)

#' @title class arrow::ipc::RecordBatchStreamReader
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname arrow__ipc__RecordBatchStreamReader
#' @name arrow__ipc__RecordBatchStreamReader
`arrow::ipc::RecordBatchStreamReader` <- R6Class("arrow::ipc::RecordBatchStreamReader", inherit = `arrow::RecordBatchReader`,
  public = list(
    batches = function() map(ipc___RecordBatchStreamReader__batches(self), shared_ptr, class = `arrow::RecordBatch`)
  )
)

#' @title class arrow::ipc::RecordBatchFileReader
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname arrow__ipc__RecordBatchFileReader
#' @name arrow__ipc__RecordBatchFileReader
`arrow::ipc::RecordBatchFileReader` <- R6Class("arrow::ipc::RecordBatchFileReader", inherit = `arrow::Object`,
  public = list(
    get_batch = function(i) shared_ptr(`arrow::RecordBatch`, ipc___RecordBatchFileReader__ReadRecordBatch(self, i)),

    batches = function() map(ipc___RecordBatchFileReader__batches(self), shared_ptr, class = `arrow::RecordBatch`)
  ),
  active = list(
    num_record_batches = function() ipc___RecordBatchFileReader__num_record_batches(self),
    schema = function() shared_ptr(`arrow::Schema`, ipc___RecordBatchFileReader__schema(self))
  )
)

#' Create a [arrow::ipc::RecordBatchStreamReader][arrow__ipc__RecordBatchStreamReader] from an input stream
#'
#' @param stream input stream, an [arrow::io::InputStream][arrow__io__InputStream] or a raw vector
#'
#' @export
RecordBatchStreamReader <- function(stream){
  UseMethod("RecordBatchStreamReader")
}

#' @export
`RecordBatchStreamReader.arrow::io::InputStream` <- function(stream) {
  shared_ptr(`arrow::ipc::RecordBatchStreamReader`, ipc___RecordBatchStreamReader__Open(stream))
}

#' @export
`RecordBatchStreamReader.raw` <- function(stream) {
  RecordBatchStreamReader(BufferReader(stream))
}

#' @export
`RecordBatchStreamReader.arrow::Buffer` <- function(stream) {
  RecordBatchStreamReader(BufferReader(stream))
}


#' Create an [arrow::ipc::RecordBatchFileReader][arrow__ipc__RecordBatchFileReader] from a file
#'
#' @param file The file to read from. A file path, or an [arrow::io::RandomAccessFile][arrow__ipc__RecordBatchFileReader]
#'
#' @export
RecordBatchFileReader <- function(file) {
  UseMethod("RecordBatchFileReader")
}

#' @export
`RecordBatchFileReader.arrow::io::RandomAccessFile` <- function(file) {
  shared_ptr(`arrow::ipc::RecordBatchFileReader`, ipc___RecordBatchFileReader__Open(file))
}

#' @export
`RecordBatchFileReader.character` <- function(file) {
  assert_that(length(file) == 1L)
  RecordBatchFileReader(ReadableFile(file))
}

#' @export
`RecordBatchFileReader.arrow::Buffer` <- function(file) {
  RecordBatchFileReader(BufferReader(file))
}

#' @export
`RecordBatchFileReader.raw` <- function(file) {
  RecordBatchFileReader(BufferReader(file))
}
