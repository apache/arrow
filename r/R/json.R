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

#' Read a JSON file
#'
#' Using [JsonTableReader]
#'
#' @inheritParams read_delim_arrow
#' @param ... Additional options, passed to `json_table_reader()`
#'
#' @return A `data.frame`, or an Table if `as_data_frame = FALSE`.
#' @export
#' @examples
#' \donttest{
#'   tf <- tempfile()
#'   on.exit(unlink(tf))
#'   writeLines('
#'     { "hello": 3.5, "world": false, "yo": "thing" }
#'     { "hello": 3.25, "world": null }
#'     { "hello": 0.0, "world": true, "yo": null }
#'   ', tf, useBytes=TRUE)
#'   df <- read_json_arrow(tf)
#' }
read_json_arrow <- function(file, col_select = NULL, as_data_frame = TRUE, ...) {
  tab <- JsonTableReader$create(file, ...)$Read()

  col_select <- enquo(col_select)
  if (!quo_is_null(col_select)) {
    tab <- tab[vars_select(names(tab), !!col_select)]
  }

  if (isTRUE(as_data_frame)) {
    tab <- as.data.frame(tab)
  }
  tab
}

#' @include arrow-package.R
#' @rdname CsvTableReader
#' @usage NULL
#' @format NULL
#' @docType class
#' @export
JsonTableReader <- R6Class("JsonTableReader", inherit = ArrowObject,
  public = list(
    Read = function() shared_ptr(Table, json___TableReader__Read(self))
  )
)
JsonTableReader$create <- function(file,
                                   read_options = JsonReadOptions$create(),
                                   parse_options = JsonParseOptions$create(),
                                   ...) {

  file <- make_readable_file(file)
  shared_ptr(
    JsonTableReader,
    json___TableReader__Make(file, read_options, parse_options)
  )
}

#' @rdname CsvReadOptions
#' @usage NULL
#' @format NULL
#' @docType class
#' @export
JsonReadOptions <- R6Class("JsonReadOptions", inherit = ArrowObject)
JsonReadOptions$create <- function(use_threads = option_use_threads(), block_size = 1048576L) {
  shared_ptr(JsonReadOptions, json___ReadOptions__initialize(use_threads, block_size))
}

#' @rdname CsvReadOptions
#' @usage NULL
#' @format NULL
#' @docType class
#' @export
JsonParseOptions <- R6Class("JsonParseOptions", inherit = ArrowObject)
JsonParseOptions$create <- function(newlines_in_values = FALSE) {
  shared_ptr(JsonParseOptions, json___ParseOptions__initialize(newlines_in_values))
}
