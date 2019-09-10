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

#' @title class arrow::Message
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
Message <- R6Class("Message", inherit = Object,
  public = list(
    Equals = function(other){
      assert_is(other, "Message")
      ipc___Message__Equals(self, other)
    },
    body_length = function() ipc___Message__body_length(self),
    Verify = function() ipc___Message__Verify(self)
  ),
  active = list(
    type = function() ipc___Message__type(self),
    metadata = function() shared_ptr(Buffer, ipc___Message__metadata(self)),
    body = function() shared_ptr(Buffer, ipc___Message__body(self))
  )
)

#' @export
`==.Message` <- function(x, y) x$Equals(y)

#' @title class arrow::MessageReader
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
MessageReader <- R6Class("MessageReader", inherit = Object,
  public = list(
    ReadNextMessage = function() unique_ptr(Message, ipc___MessageReader__ReadNextMessage(self))
  )
)

MessageReader$create <- function(stream) {
  if (!inherits(stream, "InputStream")) {
    stream <- BufferReader$create(stream)
  }
  unique_ptr(MessageReader, ipc___MessageReader__Open(stream))
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
  unique_ptr(Message, ipc___ReadMessage(stream) )
}

#' @export
read_message.MessageReader <- function(stream) {
  stream$ReadNextMessage()
}
