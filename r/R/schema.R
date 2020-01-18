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
#' s$num_fields
#' s$field(i)
#' ```
#'
#' @section Methods:
#'
#' - `$ToString()`: convert to a string
#' - `$num_fields`: returns the number of fields
#' - `$field(i)`: returns the field at index `i` (0-based)
#'
#' @rdname Schema
#' @name Schema
#' @export
Schema <- R6Class("Schema",
  inherit = Object,
  public = list(
    ToString = function() {
      fields <- print_schema_fields(self)
      if (self$HasMetadata) {
        fields <- paste0(fields, "\n\nSee $metadata for additional Schema metadata")
      }
      fields
    },
    field = function(i) shared_ptr(Field, Schema__field(self, i)),
    GetFieldByName = function(x) shared_ptr(Field, Schema__GetFieldByName(self, x)),
    serialize = function() Schema__serialize(self),
    Equals = function(other, check_metadata = TRUE) {
      Schema__Equals(self, other, isTRUE(check_metadata))
    }
  ),
  active = list(
    names = function() Schema__field_names(self),
    num_fields = function() Schema__num_fields(self),
    fields = function() map(Schema__fields(self), shared_ptr, class = Field),
    metadata = function() Schema__metadata(self),
    HasMetadata = function() Schema__HasMetadata(self)
  )
)
Schema$create <- function(...) shared_ptr(Schema, schema_(.fields(list2(...))))

print_schema_fields <- function(s) {
  # Alternative to Schema__ToString that doesn't print metadata
  paste(map_chr(s$fields, ~.$ToString()), collapse = "\n")
}

#' @param ... named list of [data types][data-type]
#' @export
#' @rdname Schema
schema <- Schema$create

#' @export
names.Schema <- function(x) x$names

#' @export
length.Schema <- function(x) x$num_fields

#' read a Schema from a stream
#'
#' @param stream a `Message`, `InputStream`, or `Buffer`
#' @param ... currently ignored
#' @return A [Schema]
#' @export
read_schema <- function(stream, ...) {
  if (inherits(stream, "Message")) {
    return(shared_ptr(Schema, ipc___ReadSchema_Message(stream)))
  } else {
    if (!inherits(stream, "InputStream")) {
      stream <- BufferReader$create(stream)
      on.exit(stream$close())
    }
    return(shared_ptr(Schema, ipc___ReadSchema_InputStream(stream)))
  }
}
