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

rowwise.arrow_dplyr_query <- function(x) {
  if (inherits(x, "arrow_dplyr_query")) {
    if (length(x$group_by_vars) > 0) {
      msg <- paste(
        "Can't re-group when creating rowwise data. Ungroup the table first",
        "before performing the rowwise operation."
      )

      abort(msg)
    }
    msg <- "Pulling data in to R first before grouping rowwise."
    warning(msg)
    .data <- compute(x)
  } else {
    .data <- x
  }

  if (".rows" %in% names(x)) {
    msg <- "Cannot have a '.rows' field in the Arrow object"
    abort(msg)
  }

  .data$rows <- seq_len(nrow(.data))
  .data <- as_adq(.data)

  .data <- group_by(.data, rows)
  .data
}

rowwise.Dataset <- rowwise.ArrowTabular <- rowwise.RecordBatchReader <- rowwise.arrow_dplyr_query
