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

#' Add the data filename as a column
#'
#' This function only exists inside `arrow` `dplyr` queries, and it only is
#' valid when quering on a `FileSystemDataset`.
#'
#' @return A `FieldRef` `Expression` that refers to the filename augmented
#' column.
#' @examples
#' \dontrun{
#' open_dataset("nyc-taxi") %>%
#'   mutate(file = add_filename())
#' }
#' @keywords internal
add_filename <- function(){

  # this currently never works because if we look at the value of
  # `caller_env(n = 2)$x` here it's an object of class FileSystemDataset (query)
  # even with a Table, e.g. arrow_table(tibble::tibble(x = 1:3)) %>% mutate(file = add_filename())
  if (!caller_env(n = 2)$x$is_dataset) {
    abort("`add_filename()` must only be called on Dataset objects.")
  }
  Expression$field_ref("__filename")
}

register_bindings_augmented <- function() {
  register_binding("arrow::add_filename", add_filename)
}
