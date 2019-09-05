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
RecordBatchReader <- R6Class("RecordBatchReader", inherit = Object,
  public = list(
    read_next_batch = function() {
      shared_ptr(RecordBatch, RecordBatchReader__ReadNext(self))
    }
  ),
  active = list(
    schema = function() shared_ptr(Schema, RecordBatchReader__schema(self))
  )
)

#' @title class arrow::RecordBatchStreamReader
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
RecordBatchStreamReader <- R6Class("RecordBatchStreamReader", inherit = RecordBatchReader,
  public = list(
    batches = function() map(ipc___RecordBatchStreamReader__batches(self), shared_ptr, class = RecordBatch)
  )
)
RecordBatchStreamReader$create <- function(stream){
  if (inherits(stream, c("raw", "Buffer"))) {
    stream <- BufferReader$create(stream)
  }
  assert_that(inherits(stream, "InputStream"))

  shared_ptr(RecordBatchStreamReader, ipc___RecordBatchStreamReader__Open(stream))
}

#' @title class arrow::RecordBatchFileReader
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
RecordBatchFileReader <- R6Class("RecordBatchFileReader", inherit = Object,
  # Why doesn't this inherit from RecordBatchReader?
  public = list(
    get_batch = function(i) shared_ptr(RecordBatch, ipc___RecordBatchFileReader__ReadRecordBatch(self, i)),

    batches = function() map(ipc___RecordBatchFileReader__batches(self), shared_ptr, class = RecordBatch)
  ),
  active = list(
    num_record_batches = function() ipc___RecordBatchFileReader__num_record_batches(self),
    schema = function() shared_ptr(Schema, ipc___RecordBatchFileReader__schema(self))
  )
)

RecordBatchFileReader$create <- function(file) {
  if (inherits(file, c("raw", "Buffer"))) {
    file <- BufferReader$create(file)
  } else if (is.character(file)) {
    assert_that(length(file) == 1L)
    file <- ReadableFile$create(file)
  }
  assert_that(inherits(file, "RandomAccessFile"))

  shared_ptr(RecordBatchFileReader, ipc___RecordBatchFileReader__Open(file))
}
