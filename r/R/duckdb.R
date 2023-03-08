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

#' Create a (virtual) DuckDB table from an Arrow object
#'
#' This will do the necessary configuration to create a (virtual) table in DuckDB
#' that is backed by the Arrow object given. No data is copied or modified until
#' `collect()` or `compute()` are called or a query is run against the table.
#'
#' The result is a dbplyr-compatible object that can be used in d(b)plyr pipelines.
#'
#' If `auto_disconnect = TRUE`, the DuckDB table that is created will be configured
#' to be unregistered when the `tbl` object is garbage collected. This is helpful
#' if you don't want to have extra table objects in DuckDB after you've finished
#' using them.
#'
#' @param .data the Arrow object (e.g. Dataset, Table) to use for the DuckDB table
#' @param con a DuckDB connection to use (default will create one and store it
#' in `options("arrow_duck_con")`)
#' @param table_name a name to use in DuckDB for this object. The default is a
#' unique string `"arrow_"` followed by numbers.
#' @param auto_disconnect should the table be automatically cleaned up when the
#' resulting object is removed (and garbage collected)? Default: `TRUE`
#'
#' @return A `tbl` of the new table in DuckDB
#'
#' @name to_duckdb
#' @export
#' @examplesIf getFromNamespace("run_duckdb_examples", "arrow")()
#' library(dplyr)
#'
#' ds <- InMemoryDataset$create(mtcars)
#'
#' ds %>%
#'   filter(mpg < 30) %>%
#'   group_by(cyl) %>%
#'   to_duckdb() %>%
#'   slice_min(disp)
to_duckdb <- function(.data,
                      con = arrow_duck_connection(),
                      table_name = unique_arrow_tablename(),
                      auto_disconnect = TRUE) {
  .data <- as_adq(.data)
  if (!requireNamespace("duckdb", quietly = TRUE)) {
    abort("Please install the `duckdb` package to pass data with `to_duckdb()`.")
  }

  duckdb::duckdb_register_arrow(con, table_name, .data)

  tbl <- dplyr::tbl(con, table_name)
  groups <- dplyr::groups(.data)
  if (length(groups)) {
    tbl <- dplyr::group_by(tbl, groups)
  }

  if (auto_disconnect) {
    # this will add the correct connection disconnection when the tbl is gced.
    # this is similar to what dbplyr does, though it calls it tbl$src$disco
    tbl$src$.arrow_finalizer_environment <- duckdb_disconnector(con, table_name)
  }

  tbl
}

arrow_duck_connection <- function() {
  con <- getOption("arrow_duck_con")
  if (is.null(con) || !DBI::dbIsValid(con)) {
    con <- DBI::dbConnect(duckdb::duckdb())
    # Use the same CPU count that the arrow library is set to
    DBI::dbExecute(con, paste0("PRAGMA threads=", cpu_count()))
    options(arrow_duck_con = con)
  }
  con
}

# helper function to determine if duckdb examples should run
# see: https://github.com/r-lib/roxygen2/issues/1242
run_duckdb_examples <- function() {
  arrow_with_dataset() &&
    requireNamespace("duckdb", quietly = TRUE) &&
    packageVersion("duckdb") > "0.2.7" &&
    requireNamespace("dplyr", quietly = TRUE) &&
    requireNamespace("dbplyr", quietly = TRUE) &&
    getRversion() >= 4
}

# Adapted from dbplyr
unique_arrow_tablename <- function() {
  i <- getOption("arrow_table_name", 0) + 1
  options(arrow_table_name = i)
  sprintf("arrow_%03i", i)
}

# Creates an environment that disconnects the database when it's GC'd
duckdb_disconnector <- function(con, tbl_name) {
  force(tbl_name)
  reg.finalizer(environment(), function(...) {
    # remote the table we ephemerally created (though only if the connection is
    # still valid)
    duckdb::duckdb_unregister_arrow(con, tbl_name)
  })
  environment()
}

#' Create an Arrow object from others
#'
#' This can be used in pipelines that pass data back and forth between Arrow and
#' other processes (like DuckDB).
#'
#' @param .data the object to be converted
#' @return A `RecordBatchReader`.
#' @export
#'
#' @examplesIf getFromNamespace("run_duckdb_examples", "arrow")()
#' library(dplyr)
#'
#' ds <- InMemoryDataset$create(mtcars)
#'
#' ds %>%
#'   filter(mpg < 30) %>%
#'   to_duckdb() %>%
#'   group_by(cyl) %>%
#'   summarize(mean_mpg = mean(mpg, na.rm = TRUE)) %>%
#'   to_arrow() %>%
#'   collect()
to_arrow <- function(.data) {
  # If this is an Arrow object already, return quickly since we're already Arrow
  if (inherits(.data, c("arrow_dplyr_query", "ArrowObject"))) {
    return(.data)
  }

  # For now, we only handle .data from duckdb, so check that it is that if we've
  # gotten this far
  if (!inherits(dbplyr::remote_con(.data), "duckdb_connection")) {
    stop(
      "to_arrow() currently only supports Arrow tables, Arrow datasets, ",
      "Arrow queries, or dbplyr tbls from duckdb connections",
      call. = FALSE
    )
  }

  # Run the query
  res <- DBI::dbSendQuery(dbplyr::remote_con(.data), dbplyr::remote_query(.data), arrow = TRUE)

  duckdb::duckdb_fetch_record_batch(res)
}
