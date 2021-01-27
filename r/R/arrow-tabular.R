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

#' @include arrow-datum.R

# Base class for RecordBatch and Table
ArrowTabular <- R6Class("ArrowTabular", inherit = ArrowObject)

#' @export
as.data.frame.ArrowTabular <- function(x, row.names = NULL, optional = FALSE, ...) {
  df <- x$to_data_frame()
  if (!is.null(r_metadata <- x$metadata$r)) {
    df <- apply_arrow_r_metadata(df, .unserialize_arrow_r_metadata(r_metadata))
  }
  df
}

#' @export
`names<-.ArrowTabular` <- function(x, value) x$RenameColumns(value)

#' @importFrom methods as
#' @export
`[.ArrowTabular` <- function(x, i, j, ..., drop = FALSE) {
  if (nargs() == 2L) {
    # List-like column extraction (x[i])
    return(x[, i])
  }
  if (!missing(j)) {
    # Selecting columns is cheaper than filtering rows, so do it first.
    # That way, if we're filtering too, we have fewer arrays to filter/slice/take
    if (is_integerish(j)) {
      if (all(j < 0)) {
        # in R, negative j means "everything but j"
        j <- setdiff(seq_len(x$num_columns), -1 * j)
      }
      x <- x$SelectColumns(as.integer(j) - 1L)
    } else if (is.character(j)) {
      x <- x$SelectColumns(match(j, names(x)) - 1L)
    }

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
`[[.ArrowTabular` <- function(x, i, ...) {
  if (is.character(i)) {
    x$GetColumnByName(i)
  } else if (is.numeric(i)) {
    x$column(i - 1)
  } else {
    stop("'i' must be character or numeric, not ", class(i), call. = FALSE)
  }
}

#' @export
`$.ArrowTabular` <- function(x, name, ...) {
  assert_that(is.string(name))
  if (name %in% ls(x)) {
    get(name, x)
  } else {
    x$GetColumnByName(name)
  }
}

#' @export
dim.ArrowTabular <- function(x) c(x$num_rows, x$num_columns)

#' @export
as.list.ArrowTabular <- function(x, ...) as.list(as.data.frame(x, ...))

#' @export
row.names.ArrowTabular <- function(x) as.character(seq_len(nrow(x)))

#' @export
dimnames.ArrowTabular <- function(x) list(row.names(x), names(x))

#' @export
head.ArrowTabular <- head.ArrowDatum

#' @export
tail.ArrowTabular <- tail.ArrowDatum

ToString_tabular <- function(x, ...) {
  # Generic to work with both RecordBatch and Table
  sch <- unlist(strsplit(x$schema$ToString(), "\n"))
  sch <- sub("(.*): (.*)", "$\\1 <\\2>", sch)
  dims <- sprintf("%s rows x %s columns", nrow(x), ncol(x))
  paste(c(dims, sch), collapse = "\n")
}
