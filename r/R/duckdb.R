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
#' Alternatively, one can pass the argument `.engine = "duckdb"` to `summarise()`
#' that starts with an Arrow object to use DuckDB to calculate the summarization
#' step. Internally, this calls `to_duckdb()` with all of the default argument
#' values.
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
#' @examplesIf arrow_with_dataset() && requireNamespace("duckdb", quietly = TRUE) && packageVersion("duckdb") > "0.2.7" && requireNamespace("dplyr", quietly = TRUE)
#' library(dplyr)
#'
#' ds <- InMemoryDataset$create(mtcars)
#'
#' ds %>%
#'   filter(mpg < 30) %>%
#'   to_duckdb() %>%
#'   group_by(cyl) %>%
#'   summarize(mean_mpg = mean(mpg, na.rm = TRUE))
#'
#' # the same query can be simplified using .engine = "duckdb"
#' ds %>%
#'   filter(mpg < 30) %>%
#'   group_by(cyl) %>%
#'   summarize(mean_mpg = mean(mpg, na.rm = TRUE), .engine = "duckdb")
#'
to_duckdb <- function(.data,
                      con = arrow_duck_connection(),
                      table_name =  unique_arrow_tablename(),
                      auto_disconnect = TRUE) {
  .data <- arrow_dplyr_query(.data)
  duckdb::duckdb_register_arrow(con, table_name, .data)

  tbl <- tbl(con, table_name)
  groups <- dplyr::groups(.data)
  if (length(groups)) {
    tbl <- dplyr::group_by(tbl, groups)
  }

  if (auto_disconnect) {
    # this will add the correct connection disconnection when the tbl is gced.
    # we should probably confirm that this use of src$disco is kosher.
    tbl$src$disco <- duckdb_disconnector(con, table_name)
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

# Adapted from dbplyr
unique_arrow_tablename <- function() {
  i <- getOption("arrow_table_name", 0) + 1
  options(arrow_table_name = i)
  sprintf("arrow_%03i", i)
}

# Creates an environment that disconnects the database when it's GC'd
duckdb_disconnector <- function(con, tbl_name) {
  reg.finalizer(environment(), function(...) {
    # remote the table we ephemerally created (though only if the connection is
    # still valid)
    if (DBI::dbIsValid(con)) {
      duckdb::duckdb_unregister_arrow(con, tbl_name)
    }

    # and there are no more tables, so we can safely shutdown
    if (length(DBI::dbListTables(con)) == 0) {
      DBI::dbDisconnect(con, shutdown=TRUE)
    }
  })
  environment()
}
