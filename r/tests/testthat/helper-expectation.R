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

expect_vector <- function(x, y, ...) {
  expect_equal(as.vector(x), y, ...)
}

expect_data_frame <- function(x, y, ...) {
  expect_equal(as.data.frame(x), y, ...)
}

expect_equivalent <- function(object, expected, ...) {
  # HACK: dplyr includes an all.equal.tbl_df method that is causing failures.
  # They look spurious, like:
  # `Can't join on 'b' x 'b' because of incompatible types (tbl_df/tbl/data.frame / tbl_df/tbl/data.frame)`
  if (tibble::is_tibble(object)) {
    class(object) <- "data.frame"
  }
  if (tibble::is_tibble(expected)) {
    class(expected) <- "data.frame"
  }
  testthat::expect_equivalent(object, expected, ...)
}

# expect_equal but for DataTypes, so the error prints better
expect_type_equal <- function(object, expected, ...) {
  if (is.Array(object)) {
    object <- object$type
  }
  if (is.Array(expected)) {
    expected <- expected$type
  }
  expect_equal(object, expected, ..., label = object$ToString(), expected.label = expected$ToString())
}

expect_match_arg_error <- function(object, values=c()) {
  expect_error(object, paste0("'arg' .*", paste(dQuote(values), collapse = ", ")))
}

expect_deprecated <- expect_warning

verify_output <- function(...) {
  if (isTRUE(grepl("conda", R.Version()$platform))) {
    skip("On conda")
  }
  testthat::verify_output(...)
}
