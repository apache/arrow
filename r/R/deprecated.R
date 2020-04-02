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

#' Read a [RecordBatch] from an encapsulated IPC message
#'
#' Deprecated in 0.17. Use [record_batch()] instead.
#'
#' @param obj a [Message], a [InputStream], a [Buffer], or a `raw` vector
#' @param schema a [schema]
#'
#' @return a [RecordBatch]
#'
#' @keywords internal
#' @export
read_record_batch <- function(obj, schema) {
  .Deprecated("record_batch")
  RecordBatch$create(obj, schema = schema)
}

#' @rdname read_ipc_stream
#' @export
#' @include ipc_stream.R
read_table <- function(x, ...) {
  .Deprecated("read_arrow")
  read_arrow(x, ..., as_data_frame = FALSE)
}
