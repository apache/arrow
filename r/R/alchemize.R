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

#' @export
alchemize_to_duckdb <- function(x, ...) {
  UseMethod("alchemize_to_duckdb")
}

#' @export
alchemize_to_python <- function(x, ...) {
  UseMethod("alchemize_to_python")
}

#' @include python.R
#' @export
alchemize_to_python.Dataset <- alchemize_to_python.arrow_dplyr_query <- function(x, ...) {
  scan <- Scanner$create(x)
  return(r_to_py(scan$ToRecordBatchReader(), ...))
}

#' @export
alchemize_to_arrow <- function(x, ...) {
  UseMethod("alchemize_to_arrow")
}

# TODO: other classes too?
#' @export
alchemize_to_arrow.pyarrow.lib.RecordBatchReader <- function(x, ...) maybe_py_to_r(x, ...)

