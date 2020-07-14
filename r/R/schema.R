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
#' - `$metadata`: returns the key-value metadata as a named list.
#'    Modify or replace by assigning in (`sch$metadata <- new_metadata`).
#'    All list elements are coerced to string.
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
    WithMetadata = function(metadata = NULL) {
      metadata <- prepare_key_value_metadata(metadata)
      shared_ptr(Schema, Schema__WithMetadata(self, metadata))
    },
    Equals = function(other, check_metadata = FALSE, ...) {
      inherits(other, "Schema") && Schema__Equals(self, other, isTRUE(check_metadata))
    }
  ),
  active = list(
    names = function() {
      out <- Schema__field_names(self)
      # Hack: Rcpp should set the encoding
      Encoding(out) <- "UTF-8"
      out
    },
    num_fields = function() Schema__num_fields(self),
    fields = function() map(Schema__fields(self), shared_ptr, class = Field),
    HasMetadata = function() Schema__HasMetadata(self),
    metadata = function(new_metadata) {
      if (missing(new_metadata)) {
        Schema__metadata(self)
      } else {
        # Set the metadata
        out <- self$WithMetadata(new_metadata)
        # $WithMetadata returns a new object but we're modifying in place,
        # so swap in that new C++ object pointer into our R6 object
        self$set_pointer(out$pointer())
        self
      }
    }
  )
)
Schema$create <- function(...) shared_ptr(Schema, schema_(.fields(list2(...))))

prepare_key_value_metadata <- function(metadata) {
  # key-value-metadata must be a named character vector;
  # this function validates and coerces
  if (is.null(metadata)) {
    # NULL to remove metadata, so equivalent to setting an empty list
    metadata <- empty_named_list()
  }
  if (is.null(names(metadata))) {
    stop(
      "Key-value metadata must be a named list or character vector",
      call. = FALSE
    )
  }
  map_chr(metadata, as.character)
}

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

#' @export
`[[.Schema` <- function(x, i, ...) {
  if (is.character(i)) {
    x$GetFieldByName(i)
  } else if (is.numeric(i)) {
    x$field(i - 1)
  } else {
    stop("'i' must be character or numeric, not ", class(i), call. = FALSE)
  }
}

#' @export
`$.Schema` <- function(x, name, ...) {
  assert_that(is.string(name))
  if (name %in% ls(x)) {
    get(name, x)
  } else {
    x$GetFieldByName(name)
  }
}

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

#' Combine and harmonize schemas
#'
#' @param ... [Schema]s to unify
#' @param schemas Alternatively, a list of schemas
#' @return A `Schema` with the union of fields contained in the inputs
#' @export
#' @examples
#' \dontrun{
#' a <- schema(b = double(), c = bool())
#' z <- schema(b = double(), k = utf8())
#' unify_schemas(a, z)
#' }
unify_schemas <- function(..., schemas = list(...)) {
  shared_ptr(Schema, arrow__UnifySchemas(schemas))
}

#' @export
print.arrow_r_metadata <- function(x, ...) {
  utils::str(x)
  utils::str(.unserialize_arrow_r_metadata(x))
  invisible(x)
}
