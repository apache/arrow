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

distinct.arrow_dplyr_query <- function(.data, ..., keep_all = FALSE) {
  browser()
  .data <- as_adq(.data)

  gv <- dplyr::group_vars(.data)
  distinct_groups <- quos(...)

  vars_to_group <- unique(c(
    unlist(lapply(distinct_groups, all.vars)), # vars referenced in summarise
    gv # vars needed for grouping
  ))

  # if there are any group_by vars, we need to add them to things to group the
  # data by

  # this grouping knackers it up
  if (length(vars_to_group) == 0) {
    .data <- dplyr::group_by(.data, !!!vars_to_group)
  } else {
    cols <- names(.data)
    .data <- dplyr::group_by(.data, !!!cols)
  }

  # df %>% select(x, y) %>% distinct() or df %>% distinct(x, y) is equivalent to
  # df %>% group_by(x, y) %>% summarize()

  .data <- summarize(.data)

  .data
}

distinct.Dataset <- distinct.ArrowTabular <- distinct.arrow_dplyr_query
