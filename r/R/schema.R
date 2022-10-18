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
#' @title Schema class
#'
#' @description A `Schema` is an Arrow object containing [Field]s, which map names to
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
#'
#' @section R Metadata:
#'
#'   When converting a data.frame to an Arrow Table or RecordBatch, attributes
#'   from the `data.frame` are saved alongside tables so that the object can be
#'   reconstructed faithfully in R (e.g. with `as.data.frame()`). This metadata
#'   can be both at the top-level of the `data.frame` (e.g. `attributes(df)`) or
#'   at the column (e.g. `attributes(df$col_a)`) or for list columns only:
#'   element level (e.g. `attributes(df[1, "col_a"])`). For example, this allows
#'   for storing `haven` columns in a table and being able to faithfully
#'   re-create them when pulled back into R. This metadata is separate from the
#'   schema (column names and types) which is compatible with other Arrow
#'   clients. The R metadata is only read by R and is ignored by other clients
#'   (e.g. Pandas has its own custom metadata). This metadata is stored in
#'   `$metadata$r`.
#'
#'   Since Schema metadata keys and values must be strings, this metadata is
#'   saved by serializing R's attribute list structure to a string. If the
#'   serialized metadata exceeds 100Kb in size, by default it is compressed
#'   starting in version 3.0.0. To disable this compression (e.g. for tables
#'   that are compatible with Arrow versions before 3.0.0 and include large
#'   amounts of metadata), set the option `arrow.compress_metadata` to `FALSE`.
#'   Files with compressed metadata are readable by older versions of arrow, but
#'   the metadata is dropped.
#'
#' @rdname Schema
#' @name Schema
#' @examples
#' schema(a = int32(), b = float64())
#'
#' schema(
#'   field("b", double()),
#'   field("c", bool(), nullable = FALSE),
#'   field("d", string())
#' )
#'
#' df <- data.frame(col1 = 2:4, col2 = c(0.1, 0.3, 0.5))
#' tab1 <- arrow_table(df)
#' tab1$schema
#' tab2 <- arrow_table(df, schema = schema(col1 = int8(), col2 = float32()))
#' tab2$schema
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
    field = function(i) Schema__field(self, i),
    GetFieldByName = function(x) Schema__GetFieldByName(self, x),
    AddField = function(i, field) {
      assert_is(field, "Field")
      Schema__AddField(self, i, field)
    },
    SetField = function(i, field) {
      assert_is(field, "Field")
      Schema__SetField(self, i, field)
    },
    RemoveField = function(i) Schema__RemoveField(self, i),
    serialize = function() Schema__serialize(self),
    WithMetadata = function(metadata = NULL) {
      metadata <- prepare_key_value_metadata(metadata)
      Schema__WithMetadata(self, metadata)
    },
    Equals = function(other, check_metadata = FALSE, ...) {
      inherits(other, "Schema") && Schema__Equals(self, other, isTRUE(check_metadata))
    },
    export_to_c = function(ptr) ExportSchema(self, ptr),
    code = function() {
      names <- self$names
      codes <- map2(names, self$fields, function(name, field) {
        field$type$code()
      })
      codes <- set_names(codes, names)

      call2("schema", !!!codes)
    }
  ),
  active = list(
    names = function() {
      Schema__field_names(self)
    },
    num_fields = function() Schema__num_fields(self),
    fields = function() Schema__fields(self),
    HasMetadata = function() Schema__HasMetadata(self),
    raw_metadata = function() {
      # This is the named list of strings
      Schema__metadata(self)
    },
    metadata = function(new_metadata) {
      if (missing(new_metadata)) {
        out <- self$raw_metadata
        if (!is.null(out[["r"]])) {
          # Can't unserialize NULL
          out[["r"]] <- .deserialize_arrow_r_metadata(out[["r"]])
        }
        out
      } else {
        # Set the metadata
        out <- self$WithMetadata(new_metadata)
        # $WithMetadata returns a new object but we're modifying in place,
        # so swap in that new C++ object pointer into our R6 object
        self$set_pointer(out$pointer())
        self
      }
    },
    r_metadata = function(new) {
      # Helper for the R metadata that handles the serialization
      # See also method on ArrowTabular
      if (missing(new)) {
        self$metadata$r
      } else {
        # Set the R metadata
        self$metadata$r <- new
        self
      }
    }
  )
)
Schema$create <- function(...) {
  .list <- list2(...)

  # if we were provided only a list of types or fields, use that
  if (length(.list) == 1 && is_list(.list[[1]])) {
    .list <- .list[[1]]
  }

  if (all(map_lgl(.list, ~ inherits(., "Field")))) {
    Schema__from_fields(.list)
  } else {
    Schema__from_list(imap(.list, as_type))
  }
}
#' @include arrowExports.R
Schema$import_from_c <- ImportSchema

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
  if (is.list(metadata[["r"]])) {
    metadata[["r"]] <- .serialize_arrow_r_metadata(metadata[["r"]])
  }
  map_chr(metadata, as.character)
}

print_schema_fields <- function(s) {
  # Alternative to Schema__ToString that doesn't print metadata
  paste(map_chr(s$fields, ~ .$ToString()), collapse = "\n")
}

#' @param ... [fields][field] or field name/[data type][data-type] pairs
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
`[[<-.Schema` <- function(x, i, value) {
  assert_that(length(i) == 1)
  if (is.character(i)) {
    field_names <- names(x)
    if (anyDuplicated(field_names)) {
      stop("Cannot update field by name with duplicates", call. = FALSE)
    }

    # If i is character, it's the field name
    if (!is.null(value) && !inherits(value, "Field")) {
      value <- field(i, as_type(value, "value"))
    }

    # No match means we're adding to the end
    i <- match(i, field_names, nomatch = length(field_names) + 1L)
  } else {
    assert_that(is.numeric(i), !is.na(i), i > 0)
    # If i is numeric and we have a type,
    # we need to grab the existing field name for the new one
    if (!is.null(value) && !inherits(value, "Field")) {
      value <- field(names(x)[i], as_type(value, "value"))
    }
  }

  i <- as.integer(i - 1L)
  if (i >= length(x)) {
    if (!is.null(value)) {
      x <- x$AddField(i, value)
    }
  } else if (is.null(value)) {
    x <- x$RemoveField(i)
  } else {
    x <- x$SetField(i, value)
  }
  x
}

#' @export
`$<-.Schema` <- `$<-.ArrowTabular`

#' @export
`[.Schema` <- function(x, i, ...) {
  if (is.logical(i)) {
    i <- rep_len(i, length(x)) # For R recycling behavior
    i <- which(i)
  }
  if (is.numeric(i)) {
    if (all(i < 0)) {
      # in R, negative i means "everything but i"
      i <- setdiff(seq_len(length(x)), -1 * i)
    }
  }
  fields <- map(i, ~ x[[.]])
  invalid <- map_lgl(fields, is.null)
  if (any(invalid)) {
    stop(
      "Invalid field name", ifelse(sum(invalid) > 1, "s: ", ": "),
      oxford_paste(i[invalid]),
      call. = FALSE
    )
  }
  Schema__from_fields(fields)
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

#' @export
as.list.Schema <- function(x, ...) x$fields

#' read a Schema from a stream
#'
#' @param stream a `Message`, `InputStream`, or `Buffer`
#' @param ... currently ignored
#' @return A [Schema]
#' @export
read_schema <- function(stream, ...) {
  if (inherits(stream, "Message")) {
    return(ipc___ReadSchema_Message(stream))
  } else {
    if (!inherits(stream, "InputStream")) {
      stream <- BufferReader$create(stream)
      on.exit(stream$close())
    }
    return(ipc___ReadSchema_InputStream(stream))
  }
}

#' Combine and harmonize schemas
#'
#' @param ... [Schema]s to unify
#' @param schemas Alternatively, a list of schemas
#' @return A `Schema` with the union of fields contained in the inputs, or
#'   `NULL` if any of `schemas` is `NULL`
#' @export
#' @examples
#' a <- schema(b = double(), c = bool())
#' z <- schema(b = double(), k = utf8())
#' unify_schemas(a, z)
unify_schemas <- function(..., schemas = list(...)) {
  if (any(vapply(schemas, is.null, TRUE))) {
    return(NULL)
  }
  arrow__UnifySchemas(schemas)
}

#' @export
print.arrow_r_metadata <- function(x, ...) {
  utils::str(x)
  utils::str(.deserialize_arrow_r_metadata(x))
  invisible(x)
}

#' Convert an object to an Arrow DataType
#'
#' @param x An object to convert to a [schema()]
#' @param ... Passed to S3 methods.
#'
#' @return A [Schema] object.
#' @export
#'
#' @examples
#' as_schema(schema(col1 = int32()))
#'
as_schema <- function(x, ...) {
  UseMethod("as_schema")
}

#' @rdname as_schema
#' @export
as_schema.Schema <- function(x, ...) {
  x
}

#' @rdname as_schema
#' @export
as_schema.StructType <- function(x, ...) {
  schema(!!!x$fields())
}

#' @export
as.data.frame.Schema <- function(x, row.names = NULL, optional = FALSE, ...) {
  as.data.frame(Table__from_schema(x))
}
