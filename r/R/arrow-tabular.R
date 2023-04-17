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

# Base class for RecordBatch and Table for S3 method dispatch only.
# Does not exist in C++ class hierarchy
ArrowTabular <- R6Class("ArrowTabular",
  inherit = ArrowObject,
  public = list(
    ToString = function() {
      sch <- unlist(strsplit(self$schema$ToString(), "\n"))
      sch <- sub("(.*): (.*)", "$\\1 <\\2>", sch)
      dims <- sprintf("%s rows x %s columns", self$num_rows, self$num_columns)
      paste(c(dims, sch), collapse = "\n")
    },
    Take = function(i) {
      if (is.numeric(i)) {
        i <- as.integer(i)
      }
      if (is.integer(i)) {
        i <- Array$create(i)
      }
      assert_that(is.Array(i))
      call_function("take", self, i)
    },
    Filter = function(i, keep_na = TRUE) {
      if (is.logical(i)) {
        i <- Array$create(i)
      }
      assert_that(is.Array(i, "bool"))
      call_function("filter", self, i, options = list(keep_na = keep_na))
    },
    SortIndices = function(names, descending = FALSE) {
      assert_that(is.character(names))
      assert_that(length(names) > 0)
      assert_that(!any(is.na(names)))
      if (length(descending) == 1L) {
        descending <- rep_len(descending, length(names))
      }
      assert_that(is.logical(descending))
      assert_that(identical(length(names), length(descending)))
      assert_that(!any(is.na(descending)))
      call_function(
        "sort_indices",
        self,
        # cpp11 does not support logical vectors so convert to integer
        options = list(names = names, orders = as.integer(descending))
      )
    }
  ),
  active = list(
    metadata = function(new) {
      if (missing(new)) {
        # Get the metadata (from the schema)
        self$schema$metadata
      } else {
        # Set the metadata
        out <- self$ReplaceSchemaMetadata(new)
        # ReplaceSchemaMetadata returns a new object but we're modifying in place,
        # so swap in that new C++ object pointer into our R6 object
        self$set_pointer(out$pointer())
        self
      }
    },
    r_metadata = function(new) {
      # Helper for the R metadata that handles the serialization
      # See also method on Schema
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

#' @export
as.data.frame.ArrowTabular <- function(x, row.names = NULL, optional = FALSE, ...) {
  df <- x$to_data_frame()
  out <- apply_arrow_r_metadata(df, x$metadata$r)
  as.data.frame(out, row.names = row.names, optional = optional, ...)
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
    if (is.character(j)) {
      j_new <- match(j, names(x))
      if (any(is.na(j_new))) {
        stop("Column not found: ", oxford_paste(j[is.na(j_new)]), call. = FALSE)
      }
      j <- j_new
    }
    if (is_integerish(j)) {
      if (any(is.na(j))) {
        stop("Column indices cannot be NA", call. = FALSE)
      }
      if (length(j) && all(j < 0)) {
        # in R, negative j means "everything but j"
        j <- setdiff(seq_len(x$num_columns), -1 * j)
      }
      x <- x$SelectColumns(as.integer(j) - 1L)
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
`[[<-.ArrowTabular` <- function(x, i, value) {
  if (!is.character(i) && !is.numeric(i)) {
    stop("'i' must be character or numeric, not ", class(i), call. = FALSE)
  }
  assert_that(length(i) == 1, !is.na(i))

  if (is.null(value)) {
    if (is.character(i)) {
      i <- match(i, names(x))
    }
    x <- x$RemoveColumn(i - 1L)
  } else {
    if (!is.character(i)) {
      # get or create a/the column name
      if (i <= x$num_columns) {
        i <- names(x)[i]
      } else {
        i <- as.character(i)
      }
    }

    # auto-magic recycling on non-ArrowObjects
    if (!inherits(value, "ArrowObject")) {
      value <- vctrs::vec_recycle(value, x$num_rows)
    }

    # construct the field
    if (inherits(x, "RecordBatch") && !inherits(value, "Array")) {
      value <- Array$create(value)
    } else if (inherits(x, "Table") && !inherits(value, "ChunkedArray")) {
      value <- ChunkedArray$create(value)
    }
    new_field <- field(i, value$type)

    if (i %in% names(x)) {
      i <- match(i, names(x)) - 1L
      x <- x$SetColumn(i, new_field, value)
    } else {
      i <- x$num_columns
      x <- x$AddColumn(i, new_field, value)
    }
  }
  x
}

#' @export
`$<-.ArrowTabular` <- function(x, i, value) {
  assert_that(is.string(i))
  # We need to check if `i` is in names in case it is an active binding (e.g.
  # `metadata`, in which case we use assign to change the active binding instead
  # of the column in the table)
  if (i %in% ls(x)) {
    assign(i, value, x)
  } else {
    x[[i]] <- value
  }
  x
}

#' @export
dim.ArrowTabular <- function(x) c(x$num_rows, x$num_columns)

#' @export
length.ArrowTabular <- function(x) x$num_columns

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

#' @export
na.fail.ArrowTabular <- function(object, ...) {
  for (col in seq_len(object$num_columns)) {
    if (object$column(col - 1L)$null_count > 0) {
      stop("missing values in object", call. = FALSE)
    }
  }
  object
}

#' @export
na.omit.ArrowTabular <- function(object, ...) {
  not_na <- map(object$columns, ~ call_function("is_valid", .x))
  not_na_agg <- Reduce("&", not_na)
  object$Filter(not_na_agg)
}

#' @export
na.exclude.ArrowTabular <- na.omit.ArrowTabular
