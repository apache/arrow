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

#' read [arrow::RecordBatch][arrow__RecordBatch] as encapsulated IPC message, given a known [arrow::Schema][arrow__Schema]
#'
#' @param obj a [arrow::ipc::Message][arrow__ipc__Message], a [arrow::io::InputStream][arrow__io__InputStream], a [arrow::Buffer][arrow__Buffer], or a raw vector
#' @param schema a [arrow::Schema][arrow__Schema]
#'
#' @return a [arrow::RecordBatch][arrow__RecordBatch]
#'
#' @export
read_record_batch <- function(obj, schema){
  UseMethod("read_record_batch")
}

#' @export
`read_record_batch.arrow::ipc::Message` <- function(obj, schema) {
  assert_that(inherits(schema, "arrow::Schema"))
  shared_ptr(`arrow::RecordBatch`, ipc___ReadRecordBatch__Message__Schema(obj, schema))
}

#' @export
`read_record_batch.arrow::io::InputStream` <- function(obj, schema) {
  assert_that(inherits(schema, "arrow::Schema"))
  shared_ptr(`arrow::RecordBatch`, ipc___ReadRecordBatch__InputStream__Schema(obj, schema))
}

#' @export
read_record_batch.raw <- function(obj, schema){
  stream <- close_on_exit(BufferReader(obj))
  read_record_batch(stream, schema)
}

#' @export
`read_record_batch.arrow::Buffer` <- function(obj, schema){
  stream <- close_on_exit(BufferReader(obj))
  read_record_batch(stream, schema)
}
