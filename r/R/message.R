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
#' @description
#' `Message` holds an Arrow IPC message, which includes metadata and
#' an optional message body.
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section R6 Methods:
#'
#' - `$Equals(other)`: Check if this `Message` is equal to another `Message`
#' - `$body_length()`: Return the length of the message body in bytes
#' - `$Verify()`: Check if the `Message` metadata is valid Flatbuffer format
#'
#' @section Active bindings:
#'
#' - `$type`: The message type
#' - `$metadata`: The message metadata
#' - `$body`: The message body as a [Buffer]
#'
#' @rdname Message
#' @name Message
Message <- R6Class(
  "Message",
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
#' @description
#' `MessageReader` reads `Message` objects from an input stream.
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section R6 Methods:
#'
#' - `$ReadNextMessage()`: Read the next `Message` from the stream. Returns `NULL` if
#'   there are no more messages.
#'
#' @section Factory:
#'
#' `MessageReader$create()` takes the following argument:
#'
#' * `stream`: An [InputStream] or object coercible to one (e.g., a raw vector)
#'
#' @rdname MessageReader
#' @name MessageReader
#' @export
MessageReader <- R6Class(
  "MessageReader",
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
