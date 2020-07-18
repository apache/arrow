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
#' @include array.R
#' @title RecordBatch class
#' @description A record batch is a collection of equal-length arrays matching
#' a particular [Schema]. It is a table-like data structure that is semantically
#' a sequence of [fields][Field], each a contiguous Arrow [Array].
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section S3 Methods and Usage:
#' Record batches are data-frame-like, and many methods you expect to work on
#' a `data.frame` are implemented for `RecordBatch`. This includes `[`, `[[`,
#' `$`, `names`, `dim`, `nrow`, `ncol`, `head`, and `tail`. You can also pull
#' the data from an Arrow record batch into R with `as.data.frame()`. See the
#' examples.
#'
#' A caveat about the `$` method: because `RecordBatch` is an `R6` object,
#' `$` is also used to access the object's methods (see below). Methods take
#' precedence over the table's columns. So, `batch$Slice` would return the
#' "Slice" method function even if there were a column in the table called
#' "Slice".
#'
#' @section R6 Methods:
#' In addition to the more R-friendly S3 methods, a `RecordBatch` object has
#' the following R6 methods that map onto the underlying C++ methods:
#'
#' - `$Equals(other)`: Returns `TRUE` if the `other` record batch is equal
#' - `$column(i)`: Extract an `Array` by integer position from the batch
#' - `$column_name(i)`: Get a column's name by integer position
#' - `$names()`: Get all column names (called by `names(batch)`)
#' - `$GetColumnByName(name)`: Extract an `Array` by string name
#' - `$RemoveColumn(i)`: Drops a column from the batch by integer position
#' - `$select(spec)`: Return a new record batch with a selection of columns.
#'    This supports the usual `character`, `numeric`, and `logical` selection
#'    methods as well as "tidy select" expressions.
#' - `$Slice(offset, length = NULL)`: Create a zero-copy view starting at the
#'    indicated integer offset and going for the given length, or to the end
#'    of the table if `NULL`, the default.
#' - `$Take(i)`: return an `RecordBatch` with rows at positions given by
#'    integers (R vector or Array Array) `i`.
#' - `$Filter(i, keep_na = TRUE)`: return an `RecordBatch` with rows at positions where logical
#'    vector (or Arrow boolean Array) `i` is `TRUE`.
#' - `$serialize()`: Returns a raw vector suitable for interprocess communication
#' - `$cast(target_schema, safe = TRUE, options = cast_options(safe))`: Alter
#'    the schema of the record batch.
#'
#' There are also some active bindings
#' - `$num_columns`
#' - `$num_rows`
#' - `$schema`
#' - `$metadata`: Returns the key-value metadata of the `Schema` as a named list.
#'    Modify or replace by assigning in (`batch$metadata <- new_metadata`).
#'    All list elements are coerced to string.
#' - `$columns`: Returns a list of `Array`s
#' @rdname RecordBatch
#' @name RecordBatch
RecordBatch <- R6Class("RecordBatch", inherit = ArrowObject,
  public = list(
    column = function(i) shared_ptr(Array, RecordBatch__column(self, i)),
    column_name = function(i) RecordBatch__column_name(self, i),
    names = function() RecordBatch__names(self),
    Equals = function(other, check_metadata = FALSE, ...) {
      inherits(other, "RecordBatch") && RecordBatch__Equals(self, other, isTRUE(check_metadata))
    },
    GetColumnByName = function(name) {
      assert_that(is.string(name))
      shared_ptr(Array, RecordBatch__GetColumnByName(self, name))
    },
    select = function(spec) {
      spec <- enquo(spec)
      if (quo_is_null(spec)) {
        self
      } else {
        all_vars <- self$names()
        vars <- vars_select(all_vars, !!spec)
        indices <- match(vars, all_vars)
        shared_ptr(RecordBatch, RecordBatch__select(self, indices))
      }
    },
    RemoveColumn = function(i){
      shared_ptr(RecordBatch, RecordBatch__RemoveColumn(self, i))
    },

    Slice = function(offset, length = NULL) {
      if (is.null(length)) {
        shared_ptr(RecordBatch, RecordBatch__Slice1(self, offset))
      } else {
        shared_ptr(RecordBatch, RecordBatch__Slice2(self, offset, length))
      }
    },
    Take = function(i) {
      if (is.numeric(i)) {
        i <- as.integer(i)
      }
      if (is.integer(i)) {
        i <- Array$create(i)
      }
      assert_is(i, "Array")
      shared_ptr(RecordBatch, call_function("take", self, i))
    },
    Filter = function(i, keep_na = TRUE) {
      if (is.logical(i)) {
        i <- Array$create(i)
      }
      shared_ptr(RecordBatch, call_function("filter", self, i, options = list(keep_na = keep_na)))
    },
    serialize = function() ipc___SerializeRecordBatch__Raw(self),
    ToString = function() ToString_tabular(self),

    cast = function(target_schema, safe = TRUE, options = cast_options(safe)) {
      assert_is(target_schema, "Schema")
      assert_is(options, "CastOptions")
      assert_that(identical(self$schema$names, target_schema$names), msg = "incompatible schemas")
      shared_ptr(RecordBatch, RecordBatch__cast(self, target_schema, options))
    }
  ),

  active = list(
    num_columns = function() RecordBatch__num_columns(self),
    num_rows = function() RecordBatch__num_rows(self),
    schema = function() shared_ptr(Schema, RecordBatch__schema(self)),
    metadata = function(new) {
      if (missing(new)) {
        # Get the metadata (from the schema)
        self$schema$metadata
      } else {
        # Set the metadata
        new <- prepare_key_value_metadata(new)
        out <- RecordBatch__ReplaceSchemaMetadata(self, new)
        # ReplaceSchemaMetadata returns a new object but we're modifying in place,
        # so swap in that new C++ object pointer into our R6 object
        self$set_pointer(out)
        self
      }
    },
    columns = function() map(RecordBatch__columns(self), shared_ptr, Array)
  )
)

RecordBatch$create <- function(..., schema = NULL) {
  arrays <- list2(...)
  if (length(arrays) == 1 && inherits(arrays[[1]], c("raw", "Buffer", "InputStream", "Message"))) {
    return(RecordBatch$from_message(arrays[[1]], schema))
  }
  # Else, list of arrays
  # making sure there are always names
  if (is.null(names(arrays))) {
    names(arrays) <- rep_len("", length(arrays))
  }
  stopifnot(length(arrays) > 0)
  # TODO: should this also assert that they're all Arrays?
  shared_ptr(RecordBatch, RecordBatch__from_arrays(schema, arrays))
}

RecordBatch$from_message <- function(obj, schema) {
  # Message/Buffer readers, previously in read_record_batch()
  assert_is(schema, "Schema")
  if (inherits(obj, c("raw", "Buffer"))) {
    obj <- BufferReader$create(obj)
    on.exit(obj$close())
  }
  if (inherits(obj, "InputStream")) {
    shared_ptr(RecordBatch, ipc___ReadRecordBatch__InputStream__Schema(obj, schema))
  } else {
    shared_ptr(RecordBatch, ipc___ReadRecordBatch__Message__Schema(obj, schema))
  }
}

#' @param ... A `data.frame` or a named set of Arrays or vectors. If given a
#' mixture of data.frames and vectors, the inputs will be autospliced together
#' (see examples). Alternatively, you can provide a single Arrow IPC
#' `InputStream`, `Message`, `Buffer`, or R `raw` object containing a `Buffer`.
#' @param schema a [Schema], or `NULL` (the default) to infer the schema from
#' the data in `...`. When providing an Arrow IPC buffer, `schema` is required.
#' @rdname RecordBatch
#' @examples
#' \donttest{
#' batch <- record_batch(name = rownames(mtcars), mtcars)
#' dim(batch)
#' dim(head(batch))
#' names(batch)
#' batch$mpg
#' batch[["cyl"]]
#' as.data.frame(batch[4:8, c("gear", "hp", "wt")])
#' }
#' @export
record_batch <- RecordBatch$create

#' @export
names.RecordBatch <- function(x) x$names()

#' @importFrom methods as
#' @export
`[.RecordBatch` <- function(x, i, j, ..., drop = FALSE) {
  if (nargs() == 2L) {
    # List-like column extraction (x[i])
    return(x[, i])
  }
  if (!missing(j)) {
    # Selecting columns is cheaper than filtering rows, so do it first.
    # That way, if we're filtering too, we have fewer arrays to filter/slice/take
    x <- x$select(j)
    if (drop && ncol(x) == 1L) {
      x <- x$column(0)
    }
  }
  if (!missing(i)) {
    x <- filter_rows(x, i, ...)
  }
  x
}

#' @export
`[[.RecordBatch` <- function(x, i, ...) {
  if (is.character(i)) {
    x$GetColumnByName(i)
  } else if (is.numeric(i)) {
    x$column(i - 1)
  } else {
    stop("'i' must be character or numeric, not ", class(i), call. = FALSE)
  }
}

#' @export
`$.RecordBatch` <- function(x, name, ...) {
  assert_that(is.string(name))
  if (name %in% ls(x)) {
    get(name, x)
  } else {
    x$GetColumnByName(name)
  }
}

#' @export
dim.RecordBatch <- function(x) {
  c(x$num_rows, x$num_columns)
}

#' @export
as.data.frame.RecordBatch <- function(x, row.names = NULL, optional = FALSE, ...) {
  df <- RecordBatch__to_dataframe(x, use_threads = option_use_threads())
  if (!is.null(r_metadata <- x$metadata$r)) {
    df <- apply_arrow_r_metadata(df, .unserialize_arrow_r_metadata(r_metadata))
  }
  df
}

.serialize_arrow_r_metadata <- function(x) {
  rawToChar(serialize(x, NULL, ascii = TRUE))
}

.unserialize_arrow_r_metadata <- function(x) {
  tryCatch(unserialize(charToRaw(x)), error = function(e) {
    warning("Invalid metadata$r", call. = FALSE)
    NULL
  })
}

apply_arrow_r_metadata <- function(x, r_metadata) {
  tryCatch({
    if (!is.null(r_metadata$attributes)) {
      attributes(x) <- r_metadata$attributes
    }

    columns_metadata <- r_metadata$columns
    if (length(names(x)) && !is.null(columns_metadata)) {
      for (name in intersect(names(columns_metadata), names(x))) {
        x[[name]] <- apply_arrow_r_metadata(x[[name]], columns_metadata[[name]])
      }
    }
  }, error = function(e) {
    warning("Invalid metadata$r", call. = FALSE)
  })
  x
}

#' @export
as.list.RecordBatch <- function(x, ...) as.list(as.data.frame(x, ...))

#' @export
row.names.RecordBatch <- function(x) as.character(seq_len(nrow(x)))

#' @export
dimnames.RecordBatch <- function(x) list(row.names(x), names(x))

#' @export
head.RecordBatch <- head.Array

#' @export
tail.RecordBatch <- tail.Array

ToString_tabular <- function(x, ...) {
  # Generic to work with both RecordBatch and Table
  sch <- unlist(strsplit(x$schema$ToString(), "\n"))
  sch <- sub("(.*): (.*)", "$\\1 <\\2>", sch)
  dims <- sprintf("%s rows x %s columns", nrow(x), ncol(x))
  paste(c(dims, sch), collapse = "\n")
}
