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
  if (.keep_all == TRUE) {
    # TODO(ARROW-14045): the function is called "hash_one" (from ARROW-13993)
    # May need to call it: `summarize(x = one(x), ...)` for x in non-group cols
    arrow_not_supported("`distinct()` with `.keep_all = TRUE`")
  }

  original_gv <- dplyr::group_vars(.data)
  if (length(quos(...))) {
    # group_by() calls mutate() if there are any expressions in ...
    .data <- dplyr::group_by(.data, ..., .add = TRUE)
  } else {
    # distinct() with no vars specified means distinct across all cols
    .data <- dplyr::group_by(.data, !!!syms(names(.data)))
  }

  out <- dplyr::summarize(.data, .groups = "drop")
  # distinct() doesn't modify group by vars, so restore the original ones
  if (length(original_gv)) {
    out$group_by_vars <- original_gv
  }
  out
}

distinct.Dataset <- distinct.ArrowTabular <- distinct.RecordBatchReader <- distinct.arrow_dplyr_query
