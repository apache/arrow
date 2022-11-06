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

#' Scan the contents of a dataset
#'
#' @description
#' A `Scanner` iterates over a [Dataset]'s fragments and returns data
#' according to given row filtering and column projection. A `ScannerBuilder`
#' can help create one.
#'
#' @section Factory:
#' `Scanner$create()` wraps the `ScannerBuilder` interface to make a `Scanner`.
#' It takes the following arguments:
#'
#' * `dataset`: A `Dataset` or `arrow_dplyr_query` object, as returned by the
#'    `dplyr` methods on `Dataset`.
#' * `projection`: A character vector of column names to select columns or a
#'    named list of expressions
#' * `filter`: A `Expression` to filter the scanned rows by, or `TRUE` (default)
#'    to keep all rows.
#' * `use_threads`: logical: should scanning use multithreading? Default `TRUE`
#' * `...`: Additional arguments, currently ignored
#' @section Methods:
#' `ScannerBuilder` has the following methods:
#'
#' - `$Project(cols)`: Indicate that the scan should only return columns given
#' by `cols`, a character vector of column names or a named list of [Expression].
#' - `$Filter(expr)`: Filter rows by an [Expression].
#' - `$UseThreads(threads)`: logical: should the scan use multithreading?
#' The method's default input is `TRUE`, but you must call the method to enable
#' multithreading because the scanner default is `FALSE`.
#' - `$BatchSize(batch_size)`: integer: Maximum row count of scanned record
#' batches, default is 32K. If scanned record batches are overflowing memory
#' then this method can be called to reduce their size.
#' - `$schema`: Active binding, returns the [Schema] of the Dataset
#' - `$Finish()`: Returns a `Scanner`
#'
#' `Scanner` currently has a single method, `$ToTable()`, which evaluates the
#' query and returns an Arrow [Table].
#' @rdname Scanner
#' @name Scanner
#' @examplesIf arrow_with_dataset() & arrow_with_parquet()
#' # Set up directory for examples
#' tf <- tempfile()
#' dir.create(tf)
#' on.exit(unlink(tf))
#'
#' write_dataset(mtcars, tf, partitioning="cyl")
#'
#' ds <- open_dataset(tf)
#'
#' scan_builder <- ds$NewScan()
#' scan_builder$Filter(Expression$field_ref("hp") > 100)
#' scan_builder$Project(list(hp_times_ten = 10 * Expression$field_ref("hp")))
#'
#' # Once configured, call $Finish()
#' scanner <- scan_builder$Finish()
#'
#' # Can get results as a table
#' as.data.frame(scanner$ToTable())
#'
#' # Or as a RecordBatchReader
#' scanner$ToRecordBatchReader()
#' @export
Scanner <- R6Class("Scanner",
  inherit = ArrowObject,
  public = list(
    ToTable = function() dataset___Scanner__ToTable(self),
    ScanBatches = function() dataset___Scanner__ScanBatches(self),
    ToRecordBatchReader = function() dataset___Scanner__ToRecordBatchReader(self),
    CountRows = function() dataset___Scanner__CountRows(self)
  ),
  active = list(
    schema = function() dataset___Scanner__schema(self)
  )
)
Scanner$create <- function(dataset,
                           projection = NULL,
                           filter = TRUE,
                           use_threads = option_use_threads(),
                           batch_size = NULL,
                           fragment_scan_options = NULL,
                           ...) {
  stop_if_no_datasets()

  if (inherits(dataset, "arrow_dplyr_query")) {
    if (is_collapsed(dataset)) {
      # TODO: Is there a way to get a RecordBatchReader rather than evaluating?
      dataset$.data <- as_adq(dplyr::compute(dataset$.data))$.data
    }

    proj <- c(dataset$selected_columns, dataset$temp_columns)

    if (!is.null(projection)) {
      if (is.character(projection)) {
        stopifnot("attempting to project with unknown columns" = all(projection %in% names(proj)))
        proj <- proj[projection]
      } else {
        # TODO: ARROW-13802 accepting lists of Expressions as a projection
        warning(
          "Scanner$create(projection = ...) must be a character vector, ",
          "ignoring the projection argument."
        )
      }
    }

    if (!isTRUE(filter)) {
      dataset <- set_filters(dataset, filter)
    }

    return(Scanner$create(
      dataset$.data,
      proj,
      dataset$filtered_rows,
      use_threads,
      batch_size,
      fragment_scan_options,
      ...
    ))
  }

  scanner_builder <- ScannerBuilder$create(dataset)
  if (use_threads) {
    scanner_builder$UseThreads()
  }
  if (!is.null(projection)) {
    scanner_builder$Project(projection)
  }
  if (!isTRUE(filter)) {
    scanner_builder$Filter(filter)
  }
  if (is_integerish(batch_size)) {
    scanner_builder$BatchSize(batch_size)
  }
  if (!is.null(fragment_scan_options)) {
    scanner_builder$FragmentScanOptions(fragment_scan_options)
  }
  scanner_builder$Finish()
}

#' @export
names.Scanner <- function(x) names(x$schema)

#' @export
head.Scanner <- function(x, n = 6L, ...) {
  assert_is(n, c("numeric", "integer"))
  assert_that(length(n) == 1)
  # Negative n requires knowing nrow(x), which requires a scan itself
  assert_that(n >= 0)
  if (!is.integer(n)) {
    n <- floor(n)
  }
  dataset___Scanner__head(x, floor(n))
}

#' @export
tail.Scanner <- function(x, n = 6L, ...) {
  tail_from_batches(dataset___Scanner__ScanBatches(x), n)$read_table()
}

tail_from_batches <- function(batches, n) {
  assert_is(n, c("numeric", "integer"))
  assert_that(length(n) == 1)
  # Negative n requires knowing nrow(x), which requires a scan itself
  assert_that(n >= 0)
  if (!is.integer(n)) {
    n <- floor(n)
  }
  result <- list()
  batch_num <- 0
  # Given a list of batches, iterate from the back
  for (batch in rev(batches)) {
    batch_num <- batch_num + 1
    result[[batch_num]] <- tail(batch, n)
    n <- n - nrow(batch)
    if (n <= 0) break
  }
  # rev() the result to put the batches back in the right order
  RecordBatchReader$create(batches = rev(result))
}

#' Apply a function to a stream of RecordBatches
#'
#' As an alternative to calling `collect()` on a `Dataset` query, you can
#' use this function to access the stream of `RecordBatch`es in the `Dataset`.
#' This lets you do more complex operations in R that operate on chunks of data
#' without having to hold the entire Dataset in memory at once. You can include
#' `map_batches()` in a dplyr pipeline and do additional dplyr methods on the
#' stream of data in Arrow after it.
#'
#' This is experimental and not recommended for production use. It is also
#' single-threaded and runs in R not C++, so it won't be as fast as core
#' Arrow methods.
#'
#' @param X A `Dataset` or `arrow_dplyr_query` object, as returned by the
#' `dplyr` methods on `Dataset`.
#' @param FUN A function or `purrr`-style lambda expression to apply to each
#' batch. It must return a RecordBatch or something coercible to one via
#' `as_record_batch()'.
#' @param .schema An optional [schema()]. If NULL, the schema will be inferred
#'   from the first batch.
#' @param .lazy Use `TRUE` to evaluate `FUN` lazily as batches are read from
#'   the result; use `FALSE` to evaluate `FUN` on all batches before returning
#'   the reader.
#' @param ... Additional arguments passed to `FUN`
#' @param .data.frame Deprecated argument, ignored
#' @return An `arrow_dplyr_query`.
#' @export
map_batches <- function(X, FUN, ..., .schema = NULL, .lazy = TRUE, .data.frame = NULL) {
  if (!is.null(.data.frame)) {
    warning(
      "The .data.frame argument is deprecated. ",
      "Call collect() on the result to get a data.frame.",
      call. = FALSE
    )
  }
  FUN <- as_mapper(FUN)
  reader <- as_record_batch_reader(X)
  dots <- list2(...)

  # If no schema is supplied, we have to evaluate the first batch here
  if (is.null(.schema)) {
    batch <- reader$read_next_batch()
    if (is.null(batch)) {
      abort("Can't infer schema from a RecordBatchReader with zero batches")
    }

    first_result <- as_record_batch(do.call(FUN, c(list(batch), dots)))
    .schema <- first_result$schema
    fun <- function() {
      if (!is.null(first_result)) {
        result <- first_result
        first_result <<- NULL
        result
      } else {
        batch <- reader$read_next_batch()
        if (is.null(batch)) {
          NULL
        } else {
          as_record_batch(
            do.call(FUN, c(list(batch), dots)),
            schema = .schema
          )
        }
      }
    }
  } else {
    fun <- function() {
      batch <- reader$read_next_batch()
      if (is.null(batch)) {
        return(NULL)
      }

      as_record_batch(
        do.call(FUN, c(list(batch), dots)),
        schema = .schema
      )
    }
  }

  reader_out <- as_record_batch_reader(fun, schema = .schema)
  if (!.lazy) {
    reader_out <- RecordBatchReader$create(
      batches = reader_out$batches(),
      schema = .schema
    )
  }

  reader_out
}

#' @usage NULL
#' @format NULL
#' @rdname Scanner
#' @export
ScannerBuilder <- R6Class("ScannerBuilder",
  inherit = ArrowObject,
  public = list(
    Project = function(cols) {
      # cols is either a character vector or a named list of Expressions
      if (is.character(cols)) {
        dataset___ScannerBuilder__ProjectNames(self, cols)
      } else if (length(cols) == 0) {
        # Empty projection
        dataset___ScannerBuilder__ProjectNames(self, character(0))
      } else {
        # List of Expressions
        dataset___ScannerBuilder__ProjectExprs(self, cols, names(cols))
      }
      self
    },
    Filter = function(expr) {
      assert_is(expr, "Expression")
      dataset___ScannerBuilder__Filter(self, expr)
      self
    },
    UseThreads = function(threads = option_use_threads()) {
      dataset___ScannerBuilder__UseThreads(self, threads)
      self
    },
    BatchSize = function(batch_size) {
      dataset___ScannerBuilder__BatchSize(self, batch_size)
      self
    },
    FragmentScanOptions = function(options) {
      dataset___ScannerBuilder__FragmentScanOptions(self, options)
      self
    },
    Finish = function() dataset___ScannerBuilder__Finish(self)
  ),
  active = list(
    schema = function() dataset___ScannerBuilder__schema(self)
  )
)
ScannerBuilder$create <- function(dataset) {
  if (inherits(dataset, "RecordBatchReader")) {
    return(dataset___ScannerBuilder__FromRecordBatchReader(dataset))
  }

  if (inherits(dataset, c("data.frame", "ArrowTabular"))) {
    dataset <- InMemoryDataset$create(dataset)
  }
  assert_is(dataset, "Dataset")

  dataset$NewScan()
}

#' @export
names.ScannerBuilder <- function(x) names(x$schema)
