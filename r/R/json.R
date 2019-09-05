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
#' Using [JsonTableReader][arrow__json__TableReader]
#'
#' @inheritParams read_delim_arrow
#' @param ... Additional options, passed to `json_table_reader()`
#'
#' @return A `data.frame`, or an `arrow::Table` if `as_tibble = FALSE`.
#' @export
#' @examples
#' \donttest{
#' try({
#'   tf <- tempfile()
#'   on.exit(unlink(tf))
#'   writeLines('
#'     { "hello": 3.5, "world": false, "yo": "thing" }
#'     { "hello": 3.25, "world": null }
#'     { "hello": 0.0, "world": true, "yo": null }
#'   ', tf, useBytes=TRUE)
#'   df <- read_json_arrow(tf)
#' })
#' }
read_json_arrow <- function(file, col_select = NULL, as_tibble = TRUE, ...) {
  tab <- json_table_reader(file, ...)$Read()$select(!!enquo(col_select))

  if (isTRUE(as_tibble)) {
    tab <- as.data.frame(tab)
  }
  tab
}

#' @include arrow-package.R
#'
#' @title class JsonTableReader
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' - `Read()` : read the JSON file as an [arrow::Table][arrow__Table]
#'
#' @rdname arrow__json__TableReader
#' @name arrow__json__TableReader
JsonTableReader <- R6Class("JsonTableReader", inherit = Object,
  public = list(
    Read = function() shared_ptr(`arrow::Table`, json___TableReader__Read(self))
  )
)
JsonTableReader$create <- function(file,
                                   read_options = json_read_options(),
                                   parse_options = json_parse_options(),
                                   ...) {

  if (is.character(file)) {
    file <- mmap_open(file)
  }
  if (inherits(file, "InputStream")) {
    file <- shared_ptr(JsonTableReader,
      json___TableReader__Make(file, read_options, parse_options)
    )
  }
  assert_that(inherits(file, c("JsonTableReader", "TableReader")))
  file
}

#' @rdname csv_table_reader
#' @export
json_table_reader <- JsonTableReader$create

JsonReadOptions <- R6Class("JsonReadOptions", inherit = Object)
JsonReadOptions$create <- function(use_threads = TRUE, block_size = 1048576L) {
  shared_ptr(JsonReadOptions, json___ReadOptions__initialize(
    list(
      use_threads = use_threads,
      block_size = block_size
    )
  ))
}

#' @rdname csv_read_options
#' @export
json_read_options <- JsonReadOptions$create

JsonParseOptions <- R6Class("JsonParseOptions", inherit = Object)
JsonParseOptions$create <- function(newlines_in_values = FALSE) {
  shared_ptr(JsonParseOptions, json___ParseOptions__initialize(
    list(
      newlines_in_values = newlines_in_values
    )
  ))
}


#' @rdname csv_parse_options
#' @export
json_parse_options <- JsonParseOptions$create
