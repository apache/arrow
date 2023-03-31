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
#' - `$nbytes()`: Total number of bytes consumed by the elements of the table
#' - `$RenameColumns(value)`: Set all column names (called by `names(tab) <- value`)
#' - `$GetColumnByName(name)`: Extract a `ChunkedArray` by string name
#' - `$field(i)`: Extract a `Field` from the table schema by integer position
#' - `$SelectColumns(indices)`: Return new `Table` with specified columns, expressed as 0-based integers.
#' - `$Slice(offset, length = NULL)`: Create a zero-copy view starting at the
#'    indicated integer offset and going for the given length, or to the end
#'    of the table if `NULL`, the default.
#' - `$Take(i)`: return an `Table` with rows at positions given by
#'    integers `i`. If `i` is an Arrow `Array` or `ChunkedArray`, it will be
#'    coerced to an R vector before taking.
#' - `$Filter(i, keep_na = TRUE)`: return an `Table` with rows at positions where logical
#'    vector or Arrow boolean-type `(Chunked)Array` `i` is `TRUE`.
#' - `$SortIndices(names, descending = FALSE)`: return an `Array` of integer row
#'    positions that can be used to rearrange the `Table` in ascending or descending
#'    order by the first named column, breaking ties with further named columns.
#'    `descending` can be a logical vector of length one or of the same length as
#'    `names`.
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
#'    All list elements are coerced to string. See `schema()` for more information.
#' - `$columns`: Returns a list of `ChunkedArray`s
#' @rdname Table
#' @name Table
#' @export
Table <- R6Class("Table",
  inherit = ArrowTabular,
  public = list(
    column = function(i) Table__column(self, i),
    ColumnNames = function() Table__ColumnNames(self),
    nbytes = function() Table__ReferencedBufferSize(self),
    RenameColumns = function(value) Table__RenameColumns(self, value),
    GetColumnByName = function(name) {
      assert_is(name, "character")
      assert_that(length(name) == 1)
      Table__GetColumnByName(self, name)
    },
    RemoveColumn = function(i) Table__RemoveColumn(self, i),
    AddColumn = function(i, new_field, value) Table__AddColumn(self, i, new_field, value),
    SetColumn = function(i, new_field, value) Table__SetColumn(self, i, new_field, value),
    ReplaceSchemaMetadata = function(new) {
      Table__ReplaceSchemaMetadata(self, prepare_key_value_metadata(new))
    },
    field = function(i) Table__field(self, i),
    serialize = function(output_stream, ...) write_table(self, output_stream, ...),
    to_data_frame = function() {
      Table__to_dataframe(self, use_threads = option_use_threads())
    },
    cast = function(target_schema, safe = TRUE, ..., options = cast_options(safe, ...)) {
      assert_is(target_schema, "Schema")
      assert_that(identical(self$schema$names, target_schema$names), msg = "incompatible schemas")
      Table__cast(self, target_schema, options)
    },
    SelectColumns = function(indices) Table__SelectColumns(self, indices),
    Slice = function(offset, length = NULL) {
      if (is.null(length)) {
        Table__Slice1(self, offset)
      } else {
        Table__Slice2(self, offset, length)
      }
    },
    # Take, Filter, and SortIndices are methods on ArrowTabular
    Equals = function(other, check_metadata = FALSE, ...) {
      inherits(other, "Table") && Table__Equals(self, other, isTRUE(check_metadata))
    },
    Validate = function() Table__Validate(self),
    ValidateFull = function() Table__ValidateFull(self)
  ),
  active = list(
    num_columns = function() Table__num_columns(self),
    num_rows = function() Table__num_rows(self),
    schema = function() Table__schema(self),
    columns = function() Table__columns(self)
  )
)

Table$create <- function(..., schema = NULL) {
  dots <- list2(...)
  # making sure there are always names
  if (is.null(names(dots))) {
    names(dots) <- rep_len("", length(dots))
  }

  if (length(dots) == 0 && inherits(schema, "Schema")) {
    return(Table__from_schema(schema))
  }

  stopifnot(length(dots) > 0)

  if (all_record_batches(dots)) {
    return(Table__from_record_batches(dots, schema))
  }
  if (length(dots) == 1 && inherits(dots[[1]], c("RecordBatchReader", "RecordBatchFileReader"))) {
    tab <- dots[[1]]$read_table()
    if (!is.null(schema)) {
      tab <- tab$cast(schema)
    }
    return(tab)
  }

  # If any arrays are length 1, recycle them
  dots <- recycle_scalars(dots)

  Table__from_dots(dots, schema, option_use_threads())
}

#' @export
names.Table <- function(x) x$ColumnNames()

#' Concatenate one or more Tables
#'
#' Concatenate one or more [Table] objects into a single table. This operation
#' does not copy array data, but instead creates new chunked arrays for each
#' column that point at existing array data.
#'
#' @param ... A [Table]
#' @param unify_schemas If TRUE, the schemas of the tables will be first unified
#' with fields of the same name being merged, then each table will be promoted
#' to the unified schema before being concatenated. Otherwise, all tables should
#' have the same schema.
#' @examples
#' tbl <- arrow_table(name = rownames(mtcars), mtcars)
#' prius <- arrow_table(name = "Prius", mpg = 58, cyl = 4, disp = 1.8)
#' combined <- concat_tables(tbl, prius)
#' tail(combined)$to_data_frame()
#' @export
concat_tables <- function(..., unify_schemas = TRUE) {
  tables <- list2(...)

  if (length(tables) == 0) {
    abort("Must pass at least one Table.")
  }

  if (!unify_schemas) {
    # assert they have same schema
    schema <- tables[[1]]$schema
    unequal_schema_idx <- which.min(lapply(tables, function(x) x$schema == schema))
    if (unequal_schema_idx != 1) {
      abort(c(
        sprintf("Schema at index %i does not match the first schema.", unequal_schema_idx),
        i = paste0("Schema 1:\n", schema$ToString()),
        i = paste0(
          sprintf("Schema %d:\n", unequal_schema_idx),
          tables[[unequal_schema_idx]]$schema$ToString()
        )
      ))
    }
  }

  Table__ConcatenateTables(tables, unify_schemas)
}

#' @export
rbind.Table <- function(...) {
  concat_tables(..., unify_schemas = FALSE)
}

#' @export
cbind.Table <- function(...) {
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

    if (inherits(input, "ArrowTabular")) {
      set_names(input$columns, names(input))
    } else if (inherits(input, "data.frame")) {
      as.list(input)
    } else {
      if (name == "") {
        abort("Vector and array arguments must have names",
          i = sprintf("Argument ..%d is missing a name", i)
        )
      }
      list2("{name}" := input)
    }
  }))

  Table$create(!!!columns)
}

#' @param ... A `data.frame` or a named set of Arrays or vectors. If given a
#' mixture of data.frames and named vectors, the inputs will be autospliced together
#' (see examples). Alternatively, you can provide a single Arrow IPC
#' `InputStream`, `Message`, `Buffer`, or R `raw` object containing a `Buffer`.
#' @param schema a [Schema], or `NULL` (the default) to infer the schema from
#' the data in `...`. When providing an Arrow IPC buffer, `schema` is required.
#' @rdname Table
#' @examples
#' tbl <- arrow_table(name = rownames(mtcars), mtcars)
#' dim(tbl)
#' dim(head(tbl))
#' names(tbl)
#' tbl$mpg
#' tbl[["cyl"]]
#' as.data.frame(tbl[4:8, c("gear", "hp", "wt")])
#' @export
arrow_table <- Table$create


#' Convert an object to an Arrow Table
#'
#' Whereas [arrow_table()] constructs a table from one or more columns,
#' `as_arrow_table()` converts a single object to an Arrow [Table].
#'
#' @param x An object to convert to an Arrow Table
#' @param ... Passed to S3 methods
#' @inheritParams arrow_table
#'
#' @return A [Table]
#' @export
#'
#' @examples
#' # use as_arrow_table() for a single object
#' as_arrow_table(data.frame(col1 = 1, col2 = "two"))
#'
#' # use arrow_table() to create from columns
#' arrow_table(col1 = 1, col2 = "two")
#'
as_arrow_table <- function(x, ..., schema = NULL) {
  UseMethod("as_arrow_table")
}

#' @rdname as_arrow_table
#' @export
as_arrow_table.default <- function(x, ...) {
  # throw a classed error here so that we can customize the error message
  # in as_writable_table()
  abort(
    sprintf(
      "No method for `as_arrow_table()` for object of class %s",
      paste(class(x), collapse = " / ")
    ),
    class = "arrow_no_method_as_arrow_table"
  )
}

#' @rdname as_arrow_table
#' @export
as_arrow_table.Table <- function(x, ..., schema = NULL) {
  if (is.null(schema)) {
    x
  } else {
    x$cast(schema)
  }
}

#' @rdname as_arrow_table
#' @export
as_arrow_table.RecordBatch <- function(x, ..., schema = NULL) {
  if (is.null(schema)) {
    Table$create(x)
  } else {
    Table$create(x$cast(schema))
  }
}

#' @rdname as_arrow_table
#' @export
as_arrow_table.data.frame <- function(x, ..., schema = NULL) {
  check_named_cols(x)
  Table$create(x, schema = schema)
}

#' @rdname as_arrow_table
#' @export
as_arrow_table.RecordBatchReader <- function(x, ...) {
  x$read_table()
}

#' @rdname as_arrow_table
#' @export
as_arrow_table.Dataset <- function(x, ...) {
  Scanner$create(x)$ToTable()
}

#' @rdname as_arrow_table
#' @export
as_arrow_table.arrow_dplyr_query <- function(x, ...) {
  reader <- as_record_batch_reader(x)
  on.exit(reader$.unsafe_delete())

  out <- as_arrow_table(reader)
  # arrow_dplyr_query holds group_by information. Set it on the table metadata.
  set_group_attributes(
    out,
    dplyr::group_vars(x),
    dplyr::group_by_drop_default(x)
  )
}

#' @rdname as_arrow_table
#' @export
as_arrow_table.Schema <- function(x, ...) {
  Table__from_schema(x)
}
