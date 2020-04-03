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
#' @description A `Schema` is a list of [Field]s, which map names to
#' Arrow [data types][data-type]. Create a `Schema` when you
#' want to convert an R `data.frame` to Arrow but don't want to rely on the
#' default mapping of R types to Arrow types, such as when you want to choose a
#' specific numeric precision, or when creating a [Dataset] and you want to
#' ensure a specific schema rather than inferring it from the various files.
#'
#' Many Arrow objects, including [Table] and [Dataset], have a `$schema` method
#' (active binding) that lets you access their schema.
#'
#' @usage NULL
#' @format NULL
#' @docType class
#' @section Methods:
#'
#' - `$ToString()`: convert to a string
#' - `$field(i)`: returns the field at index `i` (0-based)
#' - `$GetFieldByName(x)`: returns the field with name `x`
#' - `$WithMetadata(metadata)`: returns a new `Schema` with the key-value
#'    `metadata` set. Note that all list elements in `metadata` will be coerced
#'    to `character`.
#'
#' @section Active bindings:
#'
#' - `$names`: returns the field names (called in `names(Schema)`)
#' - `$num_fields`: returns the number of fields (called in `length(Schema)`)
#' - `$fields`: returns the list of `Field`s in the `Schema`, suitable for
#'   iterating over
#' - `$HasMetadata`: logical: does this `Schema` have extra metadata?
#' - `$metadata`: returns the extra metadata, if present, else `NULL`
#'
#' @rdname Schema
#' @name Schema
#' @examples
#' \donttest{
#' df <- data.frame(col1 = 2:4, col2 = c(0.1, 0.3, 0.5))
#' tab1 <- Table$create(df)
#' tab1$schema
#' tab2 <- Table$create(df, schema = schema(col1 = int8(), col2 = float32()))
#' tab2$schema
#' }
#' @export
Schema <- R6Class("Schema",
  inherit = ArrowObject,
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
    WithMetadata = function(metadata = list()) {
      # metadata must be a named character vector
      metadata <- map_chr(metadata, as.character)
      shared_ptr(Schema, Schema__WithMetadata(self, metadata))
    },
    Equals = function(other, check_metadata = FALSE, ...) {
      inherits(other, "Schema") && Schema__Equals(self, other, isTRUE(check_metadata))
    }
  ),
  active = list(
    names = function() Schema__field_names(self),
    num_fields = function() Schema__num_fields(self),
    fields = function() map(Schema__fields(self), shared_ptr, class = Field),
    HasMetadata = function() Schema__HasMetadata(self),
    metadata = function() {
      if (self$HasMetadata) {
        Schema__metadata(self)
      } else {
        NULL
      }
    }
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
