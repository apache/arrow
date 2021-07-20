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
  if (is.null(con)) {
    con <- DBI::dbConnect(duckdb::duckdb())
    # Use the same CPU count that the arrow library is set to
    # TODO: threads x2?
    DBI::dbExecute(con, paste0("PRAGMA threads=", cpu_count()))
    options(arrow_duck_con = con)
  }
  con
}

# TODO: note that this is copied from dbplyr
unique_arrow_tablename <- function () {
  i <- getOption("arrow_table_name", 0) + 1
  options(dbplyr_table_name = i)
  sprintf("arrow_%03i", i)
}

#' @export
alchemize <- function(x, ...) {
  UseMethod("alchemize")
}

#' @include python.R
alchemize_dataset <- function(x, to = c("arrow", "python", "duckdb"), ...) {
  to <- match.arg(to)

  if (to == "arrow") {
    return(x)
  } else if (to == "python") {
    scan <- Scanner$create(x)
    return(r_to_py(scan$ToRecordBatchReader(), ...))
  } else if (to == "duckdb") {
    return(rb_to_duckdb(x, ...))
  }
}

rb_to_duckdb <- function(x, con = arrow_duck_connection()) {
  table_name <- unique_arrow_tablename()
  duckdb::duckdb_register_arrow(con, table_name, x)

  tbl(con, table_name)
}

#' @export
alchemize.Dataset <- alchemize_dataset

#' @export
alchemize.arrow_dplyr_query <- alchemize_dataset



alchemize_python <- function(x, to = "arrow", ...) {
  to <- match.arg(to)

  return(maybe_py_to_r(x, ...))
}

#' @export
alchemize.pyarrow.lib.RecordBatchReader <- alchemize_python
# TODO: other classes too?
