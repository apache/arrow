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

`arrow::ipc::Message` <- R6Class("arrow::ipc::Message",
  public = list(
    body_length = function() ipc___Message__body_length(self),
    metadata = function() shared_ptr(`arrow::Buffer`, ipc__Message__metadata(self)),
    body = function() shared_ptr(`arrow::Buffer`, ipc__Message__body(self)),
    Verify = function() ipc__Message__Verify(self)
  )
)

`arrow::ipc::MessageReader` <- R6Class("arrow::ipc::MessageReader",
  public = list(
    ReadNextMessage = function() unique_ptr(`arrow::ipc::Message`, ipc___MessageReader__ReadNextMessage(self))
  )
)

#' Open a MessageReader that reads from a stream
#'
#' @param stream an InputStream
#'
#' @export
message_reader <- function(stream) {
  UseMethod("message_reader")
}

#' @export
message_reader.default <- function(stream) {
  stop("unsupported")
}

#' @export
`message_reader.arrow::io::InputStream` <- function(stream) {
  unique_ptr(`arrow::ipc::MessageReader`, ipc___MessageReader__Create(stream))
}
