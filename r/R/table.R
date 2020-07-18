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

#' @include record-batch.R
#' @title Table class
#' @description A Table is a sequence of [chunked arrays][ChunkedArray]. They
#' have a similar interface to [record batches][RecordBatch], but they can be
#' composed from multiple record batches or chunked arrays.
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Factory:
#'
#' The `Table$create()` function takes the following arguments:
#'
#' * `...` arrays, chunked arrays, or R vectors, with names; alternatively,
#'    an unnamed series of [record batches][RecordBatch] may also be provided,
#'    which will be stacked as rows in the table.
#' * `schema` a [Schema], or `NULL` (the default) to infer the schema from
#'    the data in `...`
#'
#' @section S3 Methods and Usage:
#' Tables are data-frame-like, and many methods you expect to work on
#' a `data.frame` are implemented for `Table`. This includes `[`, `[[`,
#' `$`, `names`, `dim`, `nrow`, `ncol`, `head`, and `tail`. You can also pull
#' the data from an Arrow table into R with `as.data.frame()`. See the
#' examples.
#'
#' A caveat about the `$` method: because `Table` is an `R6` object,
#' `$` is also used to access the object's methods (see below). Methods take
#' precedence over the table's columns. So, `tab$Slice` would return the
#' "Slice" method function even if there were a column in the table called
#' "Slice".
#'
#' @section R6 Methods:
#' In addition to the more R-friendly S3 methods, a `Table` object has
#' the following R6 methods that map onto the underlying C++ methods:
#'
#' - `$column(i)`: Extract a `ChunkedArray` by integer position from the table
#' - `$ColumnNames()`: Get all column names (called by `names(tab)`)
#' - `$GetColumnByName(name)`: Extract a `ChunkedArray` by string name
#' - `$field(i)`: Extract a `Field` from the table schema by integer position
#' - `$select(spec)`: Return a new table with a selection of columns.
#'    This supports the usual `character`, `numeric`, and `logical` selection
#'    methods as well as "tidy select" expressions.
#' - `$Slice(offset, length = NULL)`: Create a zero-copy view starting at the
#'    indicated integer offset and going for the given length, or to the end
#'    of the table if `NULL`, the default.
#' - `$Take(i)`: return an `Table` with rows at positions given by
#'    integers `i`. If `i` is an Arrow `Array` or `ChunkedArray`, it will be
#'    coerced to an R vector before taking.
#' - `$Filter(i, keep_na = TRUE)`: return an `Table` with rows at positions where logical
#'    vector or Arrow boolean-type `(Chunked)Array` `i` is `TRUE`.
#' - `$serialize(output_stream, ...)`: Write the table to the given
#'    [OutputStream]
#' - `$cast(target_schema, safe = TRUE, options = cast_options(safe))`: Alter
#'    the schema of the record batch.
#'
#' There are also some active bindings:
#' - `$num_columns`
#' - `$num_rows`
#' - `$schema`
#' - `$metadata`: Returns the key-value metadata of the `Schema` as a named list.
#'    Modify or replace by assigning in (`tab$metadata <- new_metadata`).
#'    All list elements are coerced to string.
#' - `$columns`: Returns a list of `ChunkedArray`s
#' @rdname Table
#' @name Table
#' @examples
#' \donttest{
#' tab <- Table$create(name = rownames(mtcars), mtcars)
#' dim(tab)
#' dim(head(tab))
#' names(tab)
#' tab$mpg
#' tab[["cyl"]]
#' as.data.frame(tab[4:8, c("gear", "hp", "wt")])
#' }
#' @export
Table <- R6Class("Table", inherit = ArrowObject,
  public = list(
    column = function(i) {
      shared_ptr(ChunkedArray, Table__column(self, i))
    },
    ColumnNames = function() Table__ColumnNames(self),
    GetColumnByName = function(name) {
      assert_is(name, "character")
      assert_that(length(name) == 1)
      shared_ptr(ChunkedArray, Table__GetColumnByName(self, name))
    },
    field = function(i) shared_ptr(Field, Table__field(self, i)),

    serialize = function(output_stream, ...) write_table(self, output_stream, ...),
    ToString = function() ToString_tabular(self),

    cast = function(target_schema, safe = TRUE, options = cast_options(safe)) {
      assert_is(target_schema, "Schema")
      assert_is(options, "CastOptions")
      assert_that(identical(self$schema$names, target_schema$names), msg = "incompatible schemas")
      shared_ptr(Table, Table__cast(self, target_schema, options))
    },

    select = function(spec) {
      spec <- enquo(spec)
      if (quo_is_null(spec)) {
        self
      } else {
        all_vars <- self$ColumnNames()
        vars <- vars_select(all_vars, !!spec)
        indices <- match(vars, all_vars)
        shared_ptr(Table, Table__select(self, indices))
      }
    },

    Slice = function(offset, length = NULL) {
      if (is.null(length)) {
        shared_ptr(Table, Table__Slice1(self, offset))
      } else {
        shared_ptr(Table, Table__Slice2(self, offset, length))
      }
    },
    Take = function(i) {
      if (is.numeric(i)) {
        i <- as.integer(i)
      }
      if (is.integer(i)) {
        i <- Array$create(i)
      }
      shared_ptr(Table, call_function("take", self, i))
    },
    Filter = function(i, keep_na = TRUE) {
      if (is.logical(i)) {
        i <- Array$create(i)
      }
      shared_ptr(Table, call_function("filter", self, i, options = list(keep_na = keep_na)))
    },

    Equals = function(other, check_metadata = FALSE, ...) {
      inherits(other, "Table") && Table__Equals(self, other, isTRUE(check_metadata))
    },

    Validate = function() {
      Table__Validate(self)
    },

    ValidateFull = function() {
      Table__ValidateFull(self)
    }
  ),

  active = list(
    num_columns = function() Table__num_columns(self),
    num_rows = function() Table__num_rows(self),
    schema = function() shared_ptr(Schema, Table__schema(self)),
    metadata = function(new) {
      if (missing(new)) {
        # Get the metadata (from the schema)
        self$schema$metadata
      } else {
        # Set the metadata
        new <- prepare_key_value_metadata(new)
        out <- Table__ReplaceSchemaMetadata(self, new)
        # ReplaceSchemaMetadata returns a new object but we're modifying in place,
        # so swap in that new C++ object pointer into our R6 object
        self$set_pointer(out)
        self
      }
    },
    columns = function() map(Table__columns(self), shared_ptr, class = ChunkedArray)
  )
)

Table$create <- function(..., schema = NULL) {
  dots <- list2(...)
  # making sure there are always names
  if (is.null(names(dots))) {
    names(dots) <- rep_len("", length(dots))
  }
  stopifnot(length(dots) > 0)
  shared_ptr(Table, Table__from_dots(dots, schema))
}

#' @export
as.data.frame.Table <- function(x, row.names = NULL, optional = FALSE, ...) {
  df <- Table__to_dataframe(x, use_threads = option_use_threads())
  if (!is.null(r_metadata <- x$metadata$r)) {
    df <- apply_arrow_r_metadata(df, .unserialize_arrow_r_metadata(r_metadata))
  }
  df
}

#' @export
as.list.Table <- as.list.RecordBatch

#' @export
row.names.Table <- row.names.RecordBatch

#' @export
dimnames.Table <- dimnames.RecordBatch

#' @export
dim.Table <- function(x) c(x$num_rows, x$num_columns)

#' @export
names.Table <- function(x) x$ColumnNames()

#' @export
`[.Table` <- `[.RecordBatch`

#' @export
`[[.Table` <- `[[.RecordBatch`

#' @export
`$.Table` <- `$.RecordBatch`

#' @export
head.Table <- head.RecordBatch

#' @export
tail.Table <- tail.RecordBatch
