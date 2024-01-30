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

compute_by <- function(by, data, ..., by_arg = "by", data_arg = "data", error_call = caller_env()) {
  check_dots_empty0(...)

  by <- enquo(by)
  check_by(by, data, by_arg = by_arg, data_arg = data_arg, error_call = error_call)

  if (is_grouped_adq(data)) {
    names <- data$group_by_vars
    from_by <- FALSE
  } else {
    names <- eval_select_by(by, data, error_call = error_call)
    from_by <- TRUE
  }

  new_by(from_by = from_by, names = names)
}

is_grouped_adq <- function(data) {
  !is_empty(data$group_by_vars)
}

check_by <- function(by,
                     data,
                     ...,
                     by_arg = "by",
                     data_arg = "data",
                     error_call = caller_env()) {
  check_dots_empty0(...)

  if (quo_is_null(by)) {
    return(invisible(NULL))
  }

  if (is_grouped_adq(data)) {
    message <- paste0(
      "Can't supply `", by_arg, "` when `",
      data_arg, "` is grouped data."
    )
    abort(message)
  }

  invisible(NULL)
}

eval_select_by <- function(by,
                           data,
                           error_call = caller_env()) {
  sim_df <- as.data.frame(implicit_schema(data))
  out <- eval_select(
    expr = by,
    data = sim_df,
    allow_rename = FALSE,
    error_call = error_call
  )
  names(out)
}

new_by <- function(from_by, names) {
  structure(list(from_by = from_by, names = names), class = "arrow_dplyr_query_by")
}
