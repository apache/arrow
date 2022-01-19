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
    invalidate = function() {
      .Call(`_arrow_RecordBatch__Reset`, self)
      super$invalidate()
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
#' @examplesIf arrow_available()
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
