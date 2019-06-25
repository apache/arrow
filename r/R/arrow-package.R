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

#' @importFrom R6 R6Class
#' @importFrom purrr map map_int map2
#' @importFrom assertthat assert_that
#' @importFrom rlang list2 %||% is_false abort dots_n warn
#' @importFrom Rcpp sourceCpp
#' @useDynLib arrow, .registration = TRUE
#' @keywords internal
"_PACKAGE"

#' Is the C++ Arrow library available?
#'
#' You won't generally need to call this function, but it's here in case it
#' helps for development purposes.
#' @return `TRUE` or `FALSE` depending on whether the package was installed
#' with the Arrow C++ library. If `FALSE`, you'll need to install the C++
#' library and then reinstall the R package. See [install_arrow()] for help.
#' @export
arrow_available <- function() {
  .Call(`_arrow_available`)
}

option_use_threads <- function() {
  !is_false(getOption("arrow.use_threads"))
}
