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

#' Transform a data structure from one engine to another
#'
#' The `alchemize_*` family of functions take data in one context (e.g. Arrow
#' data in an R session, Arrow data in a Python session) and transform it into a
#' form usable by another context: (e.g. Arrow data in a Python session, a
#' (virtual) table in a DuckDB session). All of these functions use Arrow's
#' C-interface and data is not serialized or moved when it is alchemized,
#' instead it is made available for a subprocess of the new context (e.g. Python
#' through reticulate or the DuckDB engine).
#'
#' The return value is for each function in the family based on what is at the
#' end of the function name:
#'
#' * `alchemize_to_duckdb` - returns a dbplyr-based `tbl` with the Arrow data
#' registered as a (virtual) table in DuckDB. The `tbl` can be used in dplyr
#' pipelines, or you can write DuckDB queries using the table name (by default
#' `"arrow_"` with numbers following it) given in the `tbl`. If you would like
#' to use a specific, pre-existent connection to DuckDB use the `con` argument
#' to pass the connection to use. By default, these tables are automatically
#' cleaned up when the `tbl` is removed from the session (and garbage collection
#' occurs on that), to disable this, pass `auto_disconnect = FALSE`. *
#' `alchemize_to_python` - returns a reticulate-based python object. This is the
#' same as the interface using the `r_to_py` functions.
#'
#' @param x the object to alchemize
#' @param ... arguments passed to other functions
#'
#' @return An object with a reference to the the alchemized data
#'
#' @keywords internal
#' @name alchemize
NULL

#' @rdname alchemize
#' @export
alchemize_to_duckdb <- function(x, ...) {
  UseMethod("alchemize_to_duckdb")
}

#' @rdname alchemize
#' @export
alchemize_to_python <- function(x, ...) {
  UseMethod("alchemize_to_python")
}

#' @include python.R
#' @rdname alchemize
#' @export
alchemize_to_python.Dataset <- alchemize_to_python.arrow_dplyr_query <- function(x, ...) {
  scan <- Scanner$create(x)
  return(r_to_py(scan$ToRecordBatchReader(), ...))
}

#' @rdname alchemize
#' @export
alchemize_to_arrow <- function(x, ...) {
  UseMethod("alchemize_to_arrow")
}

# TODO: other classes too?
#' @rdname alchemize
#' @export
alchemize_to_arrow.pyarrow.lib.RecordBatchReader <- function(x, ...) maybe_py_to_r(x, ...)

