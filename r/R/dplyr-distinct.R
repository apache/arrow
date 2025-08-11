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

# The following S3 methods are registered on load if dplyr is present

distinct.arrow_dplyr_query <- function(.data, ..., .keep_all = FALSE) {
  original_gv <- dplyr::group_vars(.data)
  if (length(quos(...))) {
    # group_by() calls mutate() if there are any expressions in ...
    .data <- dplyr::group_by(.data, ..., .add = TRUE)
  } else {
    # distinct() with no vars specified means distinct across all cols
    .data <- dplyr::group_by(.data, !!!syms(names(.data)))
  }

  if (isTRUE(.keep_all)) {
    # Note: in regular dplyr, `.keep_all = TRUE` returns the first row's value.
    # However, Acero's `hash_one` function prefers returning non-null values.
    # So, you'll get the same shape of data, but the values may differ.
    keeps <- names(.data)[!(names(.data) %in% .data$group_by_vars)]
    exprs <- lapply(keeps, function(x) call2("one", sym(x)))
    names(exprs) <- keeps
  } else {
    exprs <- list()
  }

  out <- dplyr::summarize(.data, !!!exprs, .groups = "drop")

  # distinct() doesn't modify group by vars, so restore the original ones
  if (length(original_gv)) {
    out$group_by_vars <- original_gv
  }
  if (isTRUE(.keep_all)) {
    # Also ensure the column order matches the original
    # summarize() will put the group_by_vars first
    out <- dplyr::select(out, !!!syms(names(.data)))
  }
  out
}

distinct.Dataset <- distinct.ArrowTabular <- distinct.RecordBatchReader <- distinct.arrow_dplyr_query
