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

# TODO: note that this is copied from dbplyr
unique_arrow_tablename <- function () {
  i <- getOption("arrow_table_name", 0) + 1
  options(arrow_table_name = i)
  sprintf("arrow_%03i", i)
}

alchemize_to_duckdb_dataset <- function(x, ...) {
  rb_to_duckdb(x, groups = x$group, ...)
}

rb_to_duckdb <- function(x, con = arrow_duck_connection(), groups = NULL, auto_disconnect = TRUE) {
  table_name <- unique_arrow_tablename()
  duckdb::duckdb_register_arrow(con, table_name, x)

  tbl <- tbl(con, table_name)
  if (length(groups) > 0 && !is.null(groups)) {
    tbl <- dplyr::group_by(tbl, !!rlang::sym(groups))
  }

  if (auto_disconnect) {
    # this will add the correct connection disconnection when the tbl is gced.
    # we should probably confirm that this use of src$disco is kosher.
    tbl$src$disco <- duckdb_disconnector(con, table_name)
  }

  tbl
}

#' @export
alchemize_to_duckdb.Dataset <- alchemize_to_duckdb_dataset

#' @export
alchemize_to_duckdb.arrow_dplyr_query <-  alchemize_to_duckdb_dataset

summarise_duck <- function(.data, ...) {
  # TODO: pass a connection?
  tbl <- alchemize_to_duckdb(.data)
  dplyr::summarise(tbl, ...)
}

# Creates an environment that disconnects the database when it's GC'd
duckdb_disconnector <- function(con, tbl_name, quiet = FALSE) {
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
