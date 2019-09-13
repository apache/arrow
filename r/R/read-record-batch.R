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

#' read [arrow::RecordBatch][RecordBatch] as encapsulated IPC message, given a known [arrow::Schema][schema]
#'
#' @param obj a [arrow::Message][Message], a [arrow::io::InputStream][InputStream], a [Buffer][buffer], or a raw vector
#' @param schema a [arrow::Schema][schema]
#'
#' @return a [arrow::RecordBatch][RecordBatch]
#'
#' @export
read_record_batch <- function(obj, schema){
  UseMethod("read_record_batch")
}

#' @export
read_record_batch.Message <- function(obj, schema) {
  assert_is(schema, "Schema")
  shared_ptr(RecordBatch, ipc___ReadRecordBatch__Message__Schema(obj, schema))
}

#' @export
read_record_batch.InputStream <- function(obj, schema) {
  assert_is(schema, "Schema")
  shared_ptr(RecordBatch, ipc___ReadRecordBatch__InputStream__Schema(obj, schema))
}

#' @export
read_record_batch.raw <- function(obj, schema){
  stream <- BufferReader$create(obj)
  on.exit(stream$close())
  read_record_batch(stream, schema)
}

#' @export
read_record_batch.Buffer <- function(obj, schema){
  stream <- BufferReader$create(obj)
  on.exit(stream$close())
  read_record_batch(stream, schema)
}
