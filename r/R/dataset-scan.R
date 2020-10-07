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
#' * `projection`: A character vector of column names to select
#' * `filter`: A `Expression` to filter the scanned rows by, or `TRUE` (default)
#'    to keep all rows.
#' * `use_threads`: logical: should scanning use multithreading? Default `TRUE`
#' * `...`: Additional arguments, currently ignored
#' @section Methods:
#' `ScannerBuilder` has the following methods:
#'
#' - `$Project(cols)`: Indicate that the scan should only return columns given
#' by `cols`, a character vector of column names
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
#' @export
Scanner <- R6Class("Scanner", inherit = ArrowObject,
  public = list(
    ToTable = function() shared_ptr(Table, dataset___Scanner__ToTable(self)),
    Scan = function() map(dataset___Scanner__Scan(self), shared_ptr, class = ScanTask)
  )
)
Scanner$create <- function(dataset,
                           projection = NULL,
                           filter = TRUE,
                           use_threads = option_use_threads(),
                           batch_size = NULL,
                           ...) {
  if (inherits(dataset, "arrow_dplyr_query") && inherits(dataset$.data, "Dataset")) {
    return(Scanner$create(
      dataset$.data,
      dataset$selected_columns,
      dataset$filtered_rows,
      use_threads,
      ...
    ))
  }
  assert_is(dataset, "Dataset")
  scanner_builder <- dataset$NewScan()
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
  scanner_builder$Finish()
}

ScanTask <- R6Class("ScanTask", inherit = ArrowObject,
  public = list(
    Execute = function() map(dataset___ScanTask__get_batches(self), shared_ptr, class = RecordBatch)
  )
)

#' Apply a function to a stream of RecordBatches
#'
#' As an alternative to calling `collect()` on a `Dataset` query, you can
#' use this function to access the stream of `RecordBatch`es in the `Dataset`.
#' This lets you aggregate on each chunk and pull the intermediate results into
#' a `data.frame` for further aggregation, even if you couldn't fit the whole
#' `Dataset` result in memory.
#'
#' This is experimental and not recommended for production use.
#'
#' @param X A `Dataset` or `arrow_dplyr_query` object, as returned by the
#' `dplyr` methods on `Dataset`.
#' @param FUN A function or `purrr`-style lambda expression to apply to each
#' batch
#' @param ... Additional arguments passed to `FUN`
#' @param .data.frame logical: collect the resulting chunks into a single
#' `data.frame`? Default `TRUE`
#' @export
map_batches <- function(X, FUN, ..., .data.frame = TRUE) {
  if (.data.frame) {
    lapply <- map_dfr
  }
  scanner <- Scanner$create(ensure_group_vars(X))
  FUN <- as_mapper(FUN)
  # message("Making ScanTasks")
  lapply(scanner$Scan(), function(scan_task) {
    # This outer lapply could be parallelized
    # message("Making Batches")
    lapply(scan_task$Execute(), function(batch) {
      # message("Processing Batch")
      # This inner lapply cannot be parallelized
      # TODO: wrap batch in arrow_dplyr_query with X$selected_columns and X$group_by_vars
      # if X is arrow_dplyr_query, if some other arg (.dplyr?) == TRUE
      FUN(batch, ...)
    })
  })
}

#' @usage NULL
#' @format NULL
#' @rdname Scanner
#' @export
ScannerBuilder <- R6Class("ScannerBuilder", inherit = ArrowObject,
  public = list(
    Project = function(cols) {
      assert_is(cols, "character")
      dataset___ScannerBuilder__Project(self, cols)
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
    Finish = function() unique_ptr(Scanner, dataset___ScannerBuilder__Finish(self))
  ),
  active = list(
    schema = function() shared_ptr(Schema, dataset___ScannerBuilder__schema(self))
  )
)

#' @export
names.ScannerBuilder <- function(x) names(x$schema)
