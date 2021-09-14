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

left_join.arrow_dplyr_query <- function(x,
                                        y,
                                        by = NULL,
                                        copy = FALSE,
                                        suffix = c(".x", ".y"),
                                        ...,
                                        keep = FALSE) {
  x <- as_adq(x)
  y <- as_adq(y)
  by <- handle_join_by(by, x, y)

  x$join <- list(
    type = JoinType[["LEFT_OUTER"]],
    right_data = y,
    by = by
  )
  collapse.arrow_dplyr_query(x)
}
left_join.Dataset <- left_join.ArrowTabular <- left_join.arrow_dplyr_query

handle_join_by <- function(by, x, y) {
  if (is.null(by)) {
    return(set_names(intersect(names(x), names(y))))
  }
  stopifnot(is.character(by))
  if (is.null(names(by))) {
    by <- set_names(by)
  }
  stopifnot(
    all(names(by) %in% names(x)),
    all(by %in% names(y))
  )
  by
}