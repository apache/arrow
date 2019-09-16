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
#' @title Schema class
#'
#' @description Create a `Schema` when you
#' want to convert an R `data.frame` to Arrow but don't want to rely on the
#' default mapping of R types to Arrow types, such as when you want to choose a
#' specific numeric precision.
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Usage:
#'
#' ```
#' s <- schema(...)
#'
#' s$ToString()
#' s$num_fields()
#' s$field(i)
#' ```
#'
#' @section Methods:
#'
#' - `$ToString()`: convert to a string
#' - `$num_fields()`: returns the number of fields
#' - `$field(i)`: returns the field at index `i` (0-based)
#'
#' @rdname Schema
#' @name Schema
#' @export
Schema <- R6Class("Schema",
  inherit = Object,
  public = list(
    ToString = function() Schema__ToString(self),
    num_fields = function() Schema__num_fields(self),
    field = function(i) shared_ptr(Field, Schema__field(self, i)),
    serialize = function() Schema__serialize(self),
    Equals = function(other, check_metadata = TRUE) Schema__Equals(self, other, isTRUE(check_metadata))
  ),
  active = list(
    names = function() Schema__names(self)
  )
)

Schema$create <- function(...) shared_ptr(Schema, schema_(.fields(list2(...))))

#' @export
`==.Schema` <- function(lhs, rhs) lhs$Equals(rhs)

#' @param ... named list of [data types][data-type]
#' @export
#' @rdname Schema
# TODO (npr): add examples once ARROW-5505 merges
schema <- Schema$create

#' read a Schema from a stream
#'
#' @param stream a stream
#' @param ... currently ignored
#'
#' @export
read_schema <- function(stream, ...) {
  UseMethod("read_schema")
}

#' @export
read_schema.InputStream <- function(stream, ...) {
  shared_ptr(Schema, ipc___ReadSchema_InputStream(stream))
}

#' @export
read_schema.Buffer <- function(stream, ...) {
  stream <- BufferReader$create(stream)
  on.exit(stream$close())
  shared_ptr(Schema, ipc___ReadSchema_InputStream(stream))
}

#' @export
read_schema.raw <- function(stream, ...) {
  stream <- BufferReader$create(stream)
  on.exit(stream$close())
  shared_ptr(Schema, ipc___ReadSchema_InputStream(stream))
}

#' @export
read_schema.Message <- function(stream, ...) {
  shared_ptr(Schema, ipc___ReadSchema_Message(stream))
}
