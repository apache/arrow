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

#' @title class arrow::RecordBatch
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname RecordBatch
#' @name RecordBatch
RecordBatch <- R6Class("RecordBatch", inherit = Object,
  public = list(
    column = function(i) {
      assert_is(i, c("numeric", "integer"))
      assert_that(length(i) == 1)
      shared_ptr(Array, RecordBatch__column(self, i))
    },
    column_name = function(i) RecordBatch__column_name(self, i),
    names = function() RecordBatch__names(self),
    Equals = function(other) {
      assert_is(other, "RecordBatch")
      RecordBatch__Equals(self, other)
    },
    GetColumnByName = function(name) {
      assert_is(name, "character")
      assert_that(length(name) == 1)
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

    serialize = function() ipc___SerializeRecordBatch__Raw(self),

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
    columns = function() map(RecordBatch__columns(self), shared_ptr, Array)
  )
)

RecordBatch$create <- function(..., schema = NULL){
  arrays <- list2(...)
  # making sure there are always names
  if (is.null(names(arrays))) {
    names(arrays) <- rep_len("", length(arrays))
  }
  stopifnot(length(arrays) > 0)
  shared_ptr(RecordBatch, RecordBatch__from_arrays(schema, arrays))
}

#' @export
names.RecordBatch <- function(x) {
  x$names()
}

#' @export
`==.RecordBatch` <- function(x, y) {
  x$Equals(y)
}

#' @export
`[.RecordBatch` <- function(x, i, j, ..., drop = FALSE) {
  if (!missing(i)) {
    if (is.numeric(i) && length(i) && all(i > 0) && all.equal(i, i[1]:i[length(i)])) {
      x <- x$Slice(i[1] - 1, length(i))
    } else {
      stop('Only RecordBatch "Slicing" (taking rows a:b) currently supported', call. = FALSE)
    }
  }
  if (!missing(j)) {
    x <- x$select(j)
    if (drop && ncol(x) == 1L) {
      x <- x$column(0)
    }
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
  assert_is(name, "character")
  assert_that(length(name) == 1L)
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
as.data.frame.RecordBatch <- function(x, row.names = NULL, optional = FALSE, use_threads = TRUE, ...){
  RecordBatch__to_dataframe(x, use_threads = option_use_threads())
}

#' @importFrom utils head
#' @export
head.RecordBatch <- function(x, n = 6L, ...) {
  assert_is(n, c("numeric", "integer"))
  assert_that(length(n) == 1)
  if (n < 0) {
    # head(x, negative) means all but the last n rows
    n <- nrow(x) + n
  }
  x$Slice(0, n)
}

#' @importFrom utils tail
#' @export
tail.RecordBatch <- function(x, n = 6L, ...) {
  assert_is(n, c("numeric", "integer"))
  assert_that(length(n) == 1)
  if (n < 0) {
    # tail(x, negative) means all but the first n rows
    n <- -n
  } else {
    n <- nrow(x) - n
  }
  x$Slice(n)
}

#' Create an [arrow::RecordBatch][RecordBatch] from a data frame
#'
#' @param ... A variable number of Array
#' @param schema a arrow::Schema
#'
#' @return a [arrow::RecordBatch][RecordBatch]
#' @export
record_batch <- RecordBatch$create
