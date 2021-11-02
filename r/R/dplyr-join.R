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

do_join <- function(x,
                    y,
                    by = NULL,
                    copy = FALSE,
                    suffix = c(".x", ".y"),
                    ...,
                    keep = FALSE,
                    na_matches,
                    join_type) {
  # TODO: handle `copy` arg: ignore?
  # TODO: handle `suffix` arg: Arrow does prefix
  # TODO: handle `keep` arg: "Should the join keys from both ‘x’ and ‘y’ be preserved in the output?"
  # TODO: handle `na_matches` arg
  x <- as_adq(x)
  y <- as_adq(y)
  by <- handle_join_by(by, x, y)

  x$join <- list(
    type = JoinType[[join_type]],
    right_data = y,
    by = by
  )
  collapse.arrow_dplyr_query(x)
}

left_join.arrow_dplyr_query <- function(x,
                                        y,
                                        by = NULL,
                                        copy = FALSE,
                                        suffix = c(".x", ".y"),
                                        ...,
                                        keep = FALSE) {
  do_join(x, y, by, copy, suffix, ..., keep = keep, join_type = "LEFT_OUTER")
}
left_join.Dataset <- left_join.ArrowTabular <- left_join.arrow_dplyr_query

right_join.arrow_dplyr_query <- function(x,
                                         y,
                                         by = NULL,
                                         copy = FALSE,
                                         suffix = c(".x", ".y"),
                                         ...,
                                         keep = FALSE) {
  do_join(x, y, by, copy, suffix, ..., keep = keep, join_type = "RIGHT_OUTER")
}
right_join.Dataset <- right_join.ArrowTabular <- right_join.arrow_dplyr_query

inner_join.arrow_dplyr_query <- function(x,
                                         y,
                                         by = NULL,
                                         copy = FALSE,
                                         suffix = c(".x", ".y"),
                                         ...,
                                         keep = FALSE) {
  do_join(x, y, by, copy, suffix, ..., keep = keep, join_type = "INNER")
}
inner_join.Dataset <- inner_join.ArrowTabular <- inner_join.arrow_dplyr_query

full_join.arrow_dplyr_query <- function(x,
                                        y,
                                        by = NULL,
                                        copy = FALSE,
                                        suffix = c(".x", ".y"),
                                        ...,
                                        keep = FALSE) {
  do_join(x, y, by, copy, suffix, ..., keep = keep, join_type = "FULL_OUTER")
}
full_join.Dataset <- full_join.ArrowTabular <- full_join.arrow_dplyr_query

semi_join.arrow_dplyr_query <- function(x,
                                        y,
                                        by = NULL,
                                        copy = FALSE,
                                        suffix = c(".x", ".y"),
                                        ...,
                                        keep = FALSE) {
  do_join(x, y, by, copy, suffix, ..., keep = keep, join_type = "LEFT_SEMI")
}
semi_join.Dataset <- semi_join.ArrowTabular <- semi_join.arrow_dplyr_query

anti_join.arrow_dplyr_query <- function(x,
                                        y,
                                        by = NULL,
                                        copy = FALSE,
                                        suffix = c(".x", ".y"),
                                        ...,
                                        keep = FALSE) {
  do_join(x, y, by, copy, suffix, ..., keep = keep, join_type = "LEFT_ANTI")
}
anti_join.Dataset <- anti_join.ArrowTabular <- anti_join.arrow_dplyr_query

handle_join_by <- function(by, x, y) {
  if (is.null(by)) {
    return(set_names(intersect(names(x), names(y))))
  }
  stopifnot(is.character(by))
  if (is.null(names(by))) {
    by <- set_names(by)
  }
  # TODO: nicer messages?
  stopifnot(
    all(names(by) %in% names(x)),
    all(by %in% names(y))
  )
  by
}
