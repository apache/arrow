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
#' - `$nbytes()`: Total number of bytes consumed by the elements of the record batch
#' - `$RenameColumns(value)`: Set all column names (called by `names(batch) <- value`)
#' - `$GetColumnByName(name)`: Extract an `Array` by string name
#' - `$RemoveColumn(i)`: Drops a column from the batch by integer position
#' - `$SelectColumns(indices)`: Return a new record batch with a selection of columns, expressed as 0-based integers.
#' - `$Slice(offset, length = NULL)`: Create a zero-copy view starting at the
#'    indicated integer offset and going for the given length, or to the end
#'    of the table if `NULL`, the default.
#' - `$Take(i)`: return an `RecordBatch` with rows at positions given by
#'    integers (R vector or Array Array) `i`.
#' - `$Filter(i, keep_na = TRUE)`: return an `RecordBatch` with rows at positions where logical
#'    vector (or Arrow boolean Array) `i` is `TRUE`.
#' - `$SortIndices(names, descending = FALSE)`: return an `Array` of integer row
#'    positions that can be used to rearrange the `RecordBatch` in ascending or
#'    descending order by the first named column, breaking ties with further named
#'    columns. `descending` can be a logical vector of length one or of the same
#'    length as `names`.
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
#'    All list elements are coerced to string. See `schema()` for more information.
#' - `$columns`: Returns a list of `Array`s
#' @rdname RecordBatch
#' @name RecordBatch
#' @export
RecordBatch <- R6Class("RecordBatch",
  inherit = ArrowTabular,
  public = list(
    column = function(i) RecordBatch__column(self, i),
    column_name = function(i) RecordBatch__column_name(self, i),
    names = function() RecordBatch__names(self),
    nbytes = function() RecordBatch__ReferencedBufferSize(self),
    RenameColumns = function(value) RecordBatch__RenameColumns(self, value),
    Equals = function(other, check_metadata = FALSE, ...) {
      inherits(other, "RecordBatch") && RecordBatch__Equals(self, other, isTRUE(check_metadata))
    },
    GetColumnByName = function(name) {
      assert_that(is.string(name))
      RecordBatch__GetColumnByName(self, name)
    },
    SelectColumns = function(indices) RecordBatch__SelectColumns(self, indices),
    AddColumn = function(i, new_field, value) {
      RecordBatch__AddColumn(self, i, new_field, value)
    },
    SetColumn = function(i, new_field, value) {
      RecordBatch__SetColumn(self, i, new_field, value)
    },
    RemoveColumn = function(i) RecordBatch__RemoveColumn(self, i),
    ReplaceSchemaMetadata = function(new) {
      RecordBatch__ReplaceSchemaMetadata(self, new)
    },
    Slice = function(offset, length = NULL) {
      if (is.null(length)) {
        RecordBatch__Slice1(self, offset)
      } else {
        RecordBatch__Slice2(self, offset, length)
      }
    },
    # Take, Filter, and SortIndices are methods on ArrowTabular
    serialize = function() ipc___SerializeRecordBatch__Raw(self),
    to_data_frame = function() {
      RecordBatch__to_dataframe(self, use_threads = option_use_threads())
    },
    cast = function(target_schema, safe = TRUE, ..., options = cast_options(safe, ...)) {
      assert_is(target_schema, "Schema")
      assert_that(identical(self$schema$names, target_schema$names), msg = "incompatible schemas")
      RecordBatch__cast(self, target_schema, options)
    },
    export_to_c = function(array_ptr, schema_ptr) {
      ExportRecordBatch(self, array_ptr, schema_ptr)
    }
  ),
  active = list(
    num_columns = function() RecordBatch__num_columns(self),
    num_rows = function() RecordBatch__num_rows(self),
    schema = function() RecordBatch__schema(self),
    columns = function() RecordBatch__columns(self)
  )
)

RecordBatch$create <- function(..., schema = NULL) {
  arrays <- list2(...)
  if (length(arrays) == 1 && inherits(arrays[[1]], c("raw", "Buffer", "InputStream", "Message"))) {
    return(RecordBatch$from_message(arrays[[1]], schema))
  }

  # Else, a list of arrays or data.frames
  # making sure there are always names
  if (is.null(names(arrays))) {
    names(arrays) <- rep_len("", length(arrays))
  }
  stopifnot(length(arrays) > 0)

  # If any arrays are length 1, recycle them
  arrays <- recycle_scalars(arrays)

  # TODO: should this also assert that they're all Arrays?
  RecordBatch__from_arrays(schema, arrays)
}

RecordBatch$from_message <- function(obj, schema) {
  # Message/Buffer readers, previously in read_record_batch()
  assert_is(schema, "Schema")
  if (inherits(obj, c("raw", "Buffer"))) {
    obj <- BufferReader$create(obj)
    on.exit(obj$close())
  }
  if (inherits(obj, "InputStream")) {
    ipc___ReadRecordBatch__InputStream__Schema(obj, schema)
  } else {
    ipc___ReadRecordBatch__Message__Schema(obj, schema)
  }
}
#' @include arrowExports.R
RecordBatch$import_from_c <- ImportRecordBatch

#' @param ... A `data.frame` or a named set of Arrays or vectors. If given a
#' mixture of data.frames and vectors, the inputs will be autospliced together
#' (see examples). Alternatively, you can provide a single Arrow IPC
#' `InputStream`, `Message`, `Buffer`, or R `raw` object containing a `Buffer`.
#' @param schema a [Schema], or `NULL` (the default) to infer the schema from
#' the data in `...`. When providing an Arrow IPC buffer, `schema` is required.
#' @rdname RecordBatch
#' @examples
#' batch <- record_batch(name = rownames(mtcars), mtcars)
#' dim(batch)
#' dim(head(batch))
#' names(batch)
#' batch$mpg
#' batch[["cyl"]]
#' as.data.frame(batch[4:8, c("gear", "hp", "wt")])
#' @export
record_batch <- RecordBatch$create

#' @export
names.RecordBatch <- function(x) x$names()

#' @export
rbind.RecordBatch <- function(...) {
  abort("Use `Table$create()` to combine RecordBatches into a Table")
}

cbind_check_length <- function(inputs, call = caller_env()) {
  sizes <- map_int(inputs, NROW)
  ok_lengths <- sizes %in% c(head(sizes, 1), 1L)
  if (!all(ok_lengths)) {
    first_bad_one <- which.min(ok_lengths)
    abort(
      c("Non-scalar inputs must have an equal number of rows.",
        i = sprintf("..1 has %d, ..%d has %d", sizes[[1]], first_bad_one, sizes[[first_bad_one]])),
      call = call
    )
  }
}

#' @export
cbind.RecordBatch <- function(...) {
  call <- sys.call()
  inputs <- list(...)
  arg_names <- if (is.null(names(inputs))) {
    rep("", length(inputs))
  } else {
    names(inputs)
  }

  cbind_check_length(inputs, call)

  columns <- flatten(map(seq_along(inputs), function(i) {
    input <- inputs[[i]]
    name <- arg_names[i]

    if (inherits(input, "RecordBatch")) {
      set_names(input$columns, names(input))
    } else if (inherits(input, "data.frame")) {
      as.list(input)
    } else if (inherits(input, "Table") || inherits(input, "ChunkedArray")) {
      abort("Cannot cbind a RecordBatch with Tables or ChunkedArrays",
            i = "Hint: consider converting the RecordBatch into a Table first")
    } else {
      if (name == "") {
        abort("Vector and array arguments must have names",
              i = sprintf("Argument ..%d is missing a name", i))
      }
      list2("{name}" := input)
    }
  }))

  RecordBatch$create(!!! columns)
}

#' Convert an object to an Arrow RecordBatch
#'
#' Whereas [record_batch()] constructs a [RecordBatch] from one or more columns,
#' `as_record_batch()` converts a single object to an Arrow [RecordBatch].
#'
#' @param x An object to convert to an Arrow RecordBatch
#' @param ... Passed to S3 methods
#' @inheritParams record_batch
#'
#' @return A [RecordBatch]
#' @export
#'
#' @examples
#' # use as_record_batch() for a single object
#' as_record_batch(data.frame(col1 = 1, col2 = "two"))
#'
#' # use record_batch() to create from columns
#' record_batch(col1 = 1, col2 = "two")
#'
as_record_batch <- function(x, ..., schema = NULL) {
  UseMethod("as_record_batch")
}

#' @rdname as_record_batch
#' @export
as_record_batch.RecordBatch <- function(x, ..., schema = NULL) {
  if (is.null(schema)) {
    x
  } else {
    x$cast(schema)
  }
}

#' @rdname as_record_batch
#' @export
as_record_batch.Table <- function(x, ..., schema = NULL) {
  if (x$num_columns == 0) {
    batch <- record_batch(data.frame())
    return(batch$Take(rep_len(0, x$num_rows)))
  }

  arrays_out <- lapply(x$columns, as_arrow_array)
  names(arrays_out) <- names(x)
  out <- RecordBatch$create(!!! arrays_out)
  if (!is.null(schema)) {
    out <- out$cast(schema)
  }

  out
}

#' @rdname as_record_batch
#' @export
as_record_batch.data.frame <- function(x, ..., schema = NULL) {
  RecordBatch$create(x, schema = schema)
}
