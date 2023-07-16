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

#' @include arrow-object.R

#' @title Message class
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname Message
#' @name Message
Message <- R6Class("Message",
  inherit = ArrowObject,
  public = list(
    Equals = function(other, ...) {
      inherits(other, "Message") && ipc___Message__Equals(self, other)
    },
    body_length = function() ipc___Message__body_length(self),
    Verify = function() ipc___Message__Verify(self)
  ),
  active = list(
    type = function() ipc___Message__type(self),
    metadata = function() ipc___Message__metadata(self),
    body = function() ipc___Message__body(self)
  )
)

#' @title MessageReader class
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname MessageReader
#' @name MessageReader
#' @export
MessageReader <- R6Class("MessageReader",
  inherit = ArrowObject,
  public = list(
    ReadNextMessage = function() ipc___MessageReader__ReadNextMessage(self)
  )
)

MessageReader$create <- function(stream) {
  if (!inherits(stream, "InputStream")) {
    stream <- BufferReader$create(stream)
  }
  ipc___MessageReader__Open(stream)
}

#' Read a Message from a stream
#'
#' @param stream an InputStream
#'
#' @export
read_message <- function(stream) {
  UseMethod("read_message")
}

#' @export
read_message.default <- function(stream) {
  read_message(BufferReader$create(stream))
}

#' @export
read_message.InputStream <- function(stream) {
  ipc___ReadMessage(stream)
}

#' @export
read_message.MessageReader <- function(stream) {
  stream$ReadNextMessage()
}
