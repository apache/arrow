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
    # `data %>% group_by() %>% summarise()` returns cols in order supplied
    # but distinct() returns cols in dataset order, so sort group vars
    .data$group_by_vars <- names(.data)[names(.data) %in% .data$group_by_vars]
  } else {
    # distinct() with no vars specified means distinct across all cols
    .data <- dplyr::group_by(.data, !!!syms(names(.data)))
  }
  if (.keep_all == TRUE) {
    # (TODO) `.keep_all = TRUE` can return first row value, but this implementation
    # do not always return it because `hash_one` skips rows if they contain null value.
    # If group vars do not uniquely determine return values of each cols,
    # the result will become different from the original.
    # If NOT, this option may distroy data.
    warning(".keep_all = TRUE currently not guarantee to take first row value in each cols.")
    keeps <- names(.data)[!(names(.data) %in% .data$group_by_vars)]
    # `one()` is wrapper for calling "hash_one" function (implemented ARROW-13993)
    # `USAGE: summarize(x = one(x), y = one(y) ...)` for x, y in non-group cols
    exprs <- lapply(keeps, function(x) call2("one", sym(x)))
    names(exprs) <- keeps
    out <- dplyr::summarize(.data, !!!exprs, .groups = "drop")
    # restore cols order
    out <- dplyr::select(out, !!!syms(names(.data)))
  } else {
    out <- dplyr::summarize(.data, .groups = "drop")
  }
  # distinct() doesn't modify group by vars, so restore the original ones
  if (length(original_gv)) {
    out$group_by_vars <- original_gv
  }
  out
}

distinct.Dataset <- distinct.ArrowTabular <- distinct.RecordBatchReader <- distinct.arrow_dplyr_query
